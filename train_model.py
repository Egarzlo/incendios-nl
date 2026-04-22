"""
Entrenamiento del modelo ML de predicción de incendios forestales — Nuevo León
==============================================================================
- Validación temporal: train 2015-2022, test 2023-2024
- Modelos: Logistic Regression, Random Forest, XGBoost
- Manejo de desbalance: class_weight / scale_pos_weight
- Métricas: ROC-AUC, PR-AUC, Recall, F1
- Exporta modelo final como .pkl
"""

import json
import logging
import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report, roc_auc_score, average_precision_score,
    precision_recall_curve, f1_score, recall_score, confusion_matrix
)
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings("ignore")

try:
    from xgboost import XGBClassifier
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Features ──────────────────────────────────────────────────────
FEATURES = [
    "temp_max", "temp_min", "humedad_min", "viento_max",
    "precipitacion", "et0",
    "dias_sin_lluvia", "dias_sin_lluvia_30d",
    "mes", "dia_del_ano", "lat", "lon", "elevacion",
    "ecoregion",
]

# Target expandido: 1 si hubo incendio ese dia o en ventana ±3d (+duracion).
# Label enriquecido reconoce que las condiciones de riesgo persisten dias
# antes/despues del inicio registrado por CONAFOR.
TARGET = "hubo_incendio_ventana"

# Threshold operativo: priorizar recall para alerta temprana. Un sistema que
# "casi nunca dice si" es inutil aunque tenga buen F1.
MIN_RECALL = 0.30


def load_and_prepare(csv_path: str):
    """Carga dataset, limpia NaN, split temporal."""
    df = pd.read_csv(csv_path)

    # Rellenar NaN con mediana
    for col in FEATURES:
        if col in df.columns and df[col].isna().any():
            df[col] = df[col].fillna(df[col].median())

    # Split temporal
    df["year"] = pd.to_datetime(df["fecha"]).dt.year
    train = df[df["year"] <= 2022].copy()
    test = df[df["year"] >= 2023].copy()

    log.info(f"Train: {len(train):,} filas, {train[TARGET].sum()} incendios ({train[TARGET].mean()*100:.3f}%)")
    log.info(f"Test:  {len(test):,} filas, {test[TARGET].sum()} incendios ({test[TARGET].mean()*100:.3f}%)")

    X_train = train[FEATURES].values
    y_train = train[TARGET].values
    X_test = test[FEATURES].values
    y_test = test[TARGET].values

    return X_train, y_train, X_test, y_test, train, test


def evaluate_model(name, model, X_test, y_test, threshold=None):
    """Evalúa modelo y retorna métricas."""
    y_proba = model.predict_proba(X_test)[:, 1]
    roc_auc = roc_auc_score(y_test, y_proba)
    pr_auc = average_precision_score(y_test, y_proba)

    # Seleccion de threshold: buscar el que maximice F1 con recall >= MIN_RECALL.
    # Si ninguno cumple, caemos al de max F1 absoluto (con warning).
    if threshold is None:
        precisions, recalls, thresholds = precision_recall_curve(y_test, y_proba)
        f1_scores = 2 * (precisions * recalls) / (precisions + recalls + 1e-8)
        # precision_recall_curve: len(thresholds) = len(precisions)-1 = len(recalls)-1
        candidates = [
            (i, f1_scores[i]) for i in range(len(thresholds))
            if recalls[i] >= MIN_RECALL
        ]
        if candidates:
            best_idx = max(candidates, key=lambda x: x[1])[0]
            log.info(f"  Threshold escogido con recall>={MIN_RECALL}: F1={f1_scores[best_idx]:.4f} recall={recalls[best_idx]:.4f}")
        else:
            best_idx = int(np.argmax(f1_scores))
            log.warning(f"  Ningun threshold alcanza recall>={MIN_RECALL}; usando max F1 absoluto")
        threshold = thresholds[min(best_idx, len(thresholds)-1)]

    y_pred = (y_proba >= threshold).astype(int)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_pred)

    log.info(f"\n{'='*60}")
    log.info(f"  {name}")
    log.info(f"{'='*60}")
    log.info(f"  ROC-AUC:  {roc_auc:.4f}")
    log.info(f"  PR-AUC:   {pr_auc:.4f}")
    log.info(f"  Threshold: {threshold:.4f}")
    log.info(f"  F1:       {f1:.4f}")
    log.info(f"  Recall:   {recall:.4f}")
    log.info(f"  Confusion Matrix:")
    log.info(f"    TN={cm[0,0]:,}  FP={cm[0,1]:,}")
    log.info(f"    FN={cm[1,0]:,}  TP={cm[1,1]:,}")

    return {
        "name": name,
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        "f1": f1,
        "recall": recall,
        "threshold": threshold,
        "model": model,
    }


def train_all_models(csv_path: str, output_dir: str = "."):
    X_train, y_train, X_test, y_test, train_df, test_df = load_and_prepare(csv_path)

    # Ratio de desbalance
    n_pos = y_train.sum()
    n_neg = len(y_train) - n_pos
    ratio = n_neg / max(n_pos, 1)
    log.info(f"Ratio negativo/positivo: {ratio:.0f}:1")

    results = []

    # ─── 1. Logistic Regression ─────────────────────────────────
    log.info("\nEntrenando Logistic Regression...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    lr = LogisticRegression(
        class_weight="balanced",
        max_iter=1000,
        C=0.5,
        random_state=42,
    )
    lr.fit(X_train_scaled, y_train)
    res_lr = evaluate_model("Logistic Regression", lr, X_test_scaled, y_test)
    res_lr["scaler"] = scaler
    results.append(res_lr)

    # ─── 2. Random Forest ───────────────────────────────────────
    log.info("\nEntrenando Random Forest...")
    rf = RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        min_samples_leaf=20,
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )
    rf.fit(X_train, y_train)
    res_rf = evaluate_model("Random Forest", rf, X_test, y_test)
    results.append(res_rf)

    # Feature importance
    importances = pd.Series(rf.feature_importances_, index=FEATURES).sort_values(ascending=False)
    log.info("\nFeature Importance (Random Forest):")
    for feat, imp in importances.items():
        log.info(f"  {feat:20s}: {imp:.4f} {'█' * int(imp * 100)}")

    # ─── 3. XGBoost ─────────────────────────────────────────────
    if HAS_XGB:
        log.info("\nEntrenando XGBoost...")
        xgb = XGBClassifier(
            n_estimators=300,
            max_depth=8,
            learning_rate=0.05,
            scale_pos_weight=ratio,
            min_child_weight=20,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1,
            eval_metric="aucpr",
            use_label_encoder=False,
        )
        xgb.fit(X_train, y_train)
        res_xgb = evaluate_model("XGBoost", xgb, X_test, y_test)
        results.append(res_xgb)

    # ─── Seleccionar mejor modelo ───────────────────────────────
    best = max(results, key=lambda r: r["pr_auc"])
    log.info(f"\n{'*'*60}")
    log.info(f"  MEJOR MODELO: {best['name']}")
    log.info(f"  PR-AUC: {best['pr_auc']:.4f}, ROC-AUC: {best['roc_auc']:.4f}")
    log.info(f"  Threshold óptimo: {best['threshold']:.4f}")
    log.info(f"{'*'*60}")

    # ─── Exportar ───────────────────────────────────────────────
    model_data = {
        "model": best["model"],
        "features": FEATURES,
        "threshold": best["threshold"],
        "model_name": best["name"],
        "metrics": {
            "roc_auc": best["roc_auc"],
            "pr_auc": best["pr_auc"],
            "f1": best["f1"],
            "recall": best["recall"],
        },
        "version": "ml_v1",
    }

    # Si el mejor fue Logistic Regression, incluir scaler
    if best["name"] == "Logistic Regression":
        model_data["scaler"] = res_lr["scaler"]

    model_path = f"{output_dir}/modelo_incendios_nl.pkl"
    joblib.dump(model_data, model_path)
    log.info(f"\nModelo exportado: {model_path}")

    # Exportar metadata como JSON legible
    meta = {
        "model_name": best["name"],
        "version": "ml_v1",
        "features": FEATURES,
        "threshold": round(best["threshold"], 6),
        "metrics": {
            "roc_auc": round(best["roc_auc"], 4),
            "pr_auc": round(best["pr_auc"], 4),
            "f1": round(best["f1"], 4),
            "recall": round(best["recall"], 4),
        },
        "training_period": "2015-2022",
        "test_period": "2023-2024",
        "n_train": len(y_train),
        "n_test": len(y_test),
        "n_fires_train": int(y_train.sum()),
        "n_fires_test": int(y_test.sum()),
        "class_ratio": f"{ratio:.0f}:1",
    }
    meta_path = f"{output_dir}/modelo_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    log.info(f"Metadata:       {meta_path}")

    return best, results


if __name__ == "__main__":
    from pathlib import Path

    script_dir = Path(__file__).parent
    csv_path = str(script_dir / "data" / "training_dataset_incendios_nl.csv")

    if not Path(csv_path).exists():
        print(f"ERROR: No se encontró el dataset: {csv_path}")
        print("Ejecuta primero: python build_dataset_local.py (o build_training_dataset.py)")
        exit(1)

    print(f"Dataset: {csv_path}")
    print(f"Output:  {script_dir}")

    best, results = train_all_models(
        csv_path=csv_path,
        output_dir=str(script_dir),
    )

    # Resumen final
    print("\n\n╔══════════════════════════════════════════════════════╗")
    print("║  RESUMEN DE MODELOS                                 ║")
    print("╠══════════════════════════════════════════════════════╣")
    for r in results:
        star = " ★" if r["name"] == best["name"] else ""
        print(f"║  {r['name']:25s} ROC={r['roc_auc']:.4f}  PR={r['pr_auc']:.4f}{star:3s} ║")
    print("╚══════════════════════════════════════════════════════╝")
