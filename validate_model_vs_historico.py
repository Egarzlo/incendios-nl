"""
Validacion del modelo ML contra incendios reales historicos.

Para cada mes (ej. abril 2023 y 2024 del test set) compara la probabilidad
que predice el modelo en dias-muni con incendio real vs dias-muni sin
incendio. Un buen modelo deberia asignar prob sustancialmente mayor a los
dias con incendio.

Uso: python validate_model_vs_historico.py
"""
import numpy as np
import pandas as pd
import joblib
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
MODEL_PATH = SCRIPT_DIR / "modelo_incendios_nl.pkl"
DATA_PATH = SCRIPT_DIR / "data" / "training_dataset_incendios_nl.csv"

def main():
    model_data = joblib.load(MODEL_PATH)
    features = model_data["features"]
    threshold = model_data.get("threshold", 0.5)
    model = model_data["model"]
    scaler = model_data.get("scaler")

    df = pd.read_csv(DATA_PATH)
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['year'] = df['fecha'].dt.year
    df['mes'] = df['fecha'].dt.month

    # Test set: 2023-2024
    test = df[df['year'] >= 2023].copy()
    X = test[features].fillna(test[features].median()).values
    if scaler is not None:
        X = scaler.transform(X)
    test['prob'] = model.predict_proba(X)[:, 1]

    print(f"{'='*70}")
    print(f"Modelo: {model_data.get('model_name','?')} v{model_data.get('version','?')}")
    print(f"Threshold operativo: {threshold:.4f}")
    print(f"{'='*70}\n")

    # 1. Comparacion prob media en fire-days vs no-fire-days
    fire_days = test[test['hubo_incendio'] == 1]
    no_fire = test[test['hubo_incendio'] == 0]
    print(f"Test set (2023-2024): {len(test):,} filas, {len(fire_days)} incendios reales")
    print(f"  Prob media en dia CON incendio (exacto): {fire_days['prob'].mean():.4f}")
    print(f"  Prob media en dia SIN incendio:           {no_fire['prob'].mean():.4f}")
    print(f"  Ratio:                                    {fire_days['prob'].mean()/max(no_fire['prob'].mean(),1e-9):.1f}x\n")

    # 2. Mismo analisis en ventana (+-3d)
    if 'hubo_incendio_ventana' in test.columns:
        fv = test[test['hubo_incendio_ventana'] == 1]
        nv = test[test['hubo_incendio_ventana'] == 0]
        print(f"Label ventana (±3d):")
        print(f"  Prob media en ventana:  {fv['prob'].mean():.4f}")
        print(f"  Prob media fuera:       {nv['prob'].mean():.4f}\n")

    # 3. Por mes: cuantos fuegos reales hubo y con que prob promedio los predice el modelo
    print("Por mes (test 2023-2024):")
    print(f"  {'Mes':>4}  {'#Fuegos':>8}  {'prob_media_fuego':>18}  {'prob_media_nofuego':>19}  {'ratio':>6}")
    for mes in range(1, 13):
        m = test[test['mes'] == mes]
        if not len(m): continue
        f = m[m['hubo_incendio']==1]
        nf = m[m['hubo_incendio']==0]
        if not len(f): continue
        ratio = f['prob'].mean()/max(nf['prob'].mean(), 1e-9)
        print(f"  {mes:>4}  {len(f):>8}  {f['prob'].mean():>18.4f}  {nf['prob'].mean():>19.4f}  {ratio:>6.1f}x")

    # 4. Top 20 dias-muni con mayor prob en abril test, ver si coinciden con fuegos reales
    print("\nTop 20 predicciones mas altas en ABRIL test 2023-2024:")
    abr = test[test['mes']==4].sort_values('prob', ascending=False).head(20)
    print(f"  {'fecha':10}  {'muni':26}  {'prob':>6}  {'fuego_real?':>12}")
    for _, r in abr.iterrows():
        fuego = 'SI' if r['hubo_incendio']==1 else ('(±3d)' if r.get('hubo_incendio_ventana',0)==1 else 'no')
        print(f"  {r['fecha'].strftime('%Y-%m-%d'):10}  {r['municipio'][:26]:26}  {r['prob']:>6.3f}  {fuego:>12}")

    # 5. Recall real a varios thresholds sobre label estricto
    print("\nRecall/precision a distintos thresholds (test 2023-2024, label estricto):")
    print(f"  {'thr':>6}  {'precision':>10}  {'recall':>8}  {'f1':>6}  {'TP':>4}  {'FP':>5}  {'FN':>4}")
    for thr in [0.1, 0.2, 0.3, 0.4, 0.5, threshold]:
        pred = (test['prob'] >= thr).astype(int)
        tp = ((pred==1) & (test['hubo_incendio']==1)).sum()
        fp = ((pred==1) & (test['hubo_incendio']==0)).sum()
        fn = ((pred==0) & (test['hubo_incendio']==1)).sum()
        prec = tp/max(tp+fp,1)
        rec = tp/max(tp+fn,1)
        f1 = 2*prec*rec/max(prec+rec,1e-9)
        marker = " <-- threshold operativo" if abs(thr-threshold)<0.001 else ""
        print(f"  {thr:>6.3f}  {prec:>10.3f}  {rec:>8.3f}  {f1:>6.3f}  {tp:>4}  {fp:>5}  {fn:>4}{marker}")


if __name__ == "__main__":
    main()
