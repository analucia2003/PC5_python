import pandas as pd
import sqlite3


data_wine = pd.read_csv('data/winemag-data-130k-v2.csv')

data_wine.rename(columns={
    'country': 'pais',
    'description': 'descripcion',
    'points': 'puntuacion',
    'price': 'precio'
}, inplace=True)


data_wine['rango_precio'] = pd.cut(data_wine['precio'], bins=[0, 20, 50, 100, 200, 1000], labels=['Bajo', 'Medio', 'Alto', 'Premium', 'Lujoso'], right=False)

data_wine['puntuacion_clasificacion'] = pd.cut(data_wine['puntuacion'], bins=[0, 85, 90, 95, 100], labels=['Regular', 'Bueno', 'Excelente', 'Sobresaliente'], right=False)


def asignar_continente(pais):
    if pais in ['Italy', 'France', 'Spain', 'Portugal', 'Germany']:
        return 'Europa'
    elif pais in ['US', 'Canada']:
        return 'América del Norte'
    elif pais in ['Argentina', 'Chile']:
        return 'América del Sur'
    elif pais in ['Australia', 'New Zealand']:
        return 'Oceanía'
    elif pais in ['South Africa']:
        return 'África'
    else:
        return 'Desconocido'

data_wine['continente'] = data_wine['pais'].apply(asignar_continente)


reporte_1 = data_wine.groupby('continente').apply(lambda x: x.nlargest(1, 'puntuacion'))[['pais', 'puntuacion', 'variety', 'winery']]
reporte_1.to_csv('reporte_1_mejores_puntuaciones.csv', index=False)

reporte_2 = data_wine.groupby('pais').agg({'precio': 'mean'}).sort_values(by='precio', ascending=False)

reporte_2.to_json('reporte_2_precio_promedio_reviews.json')

reporte_3 = data_wine.groupby('rango_precio').size().reset_index(name='cantidad_vinos')

reporte_3.to_excel('reporte_3_vinos_por_rango_precio.xlsx')

reporte_4 = data_wine['variety'].value_counts().reset_index(name='cantidad').rename(columns={'index': 'variety'})
conn = sqlite3.connect('reporte_4_variedades.sqlite')
reporte_4.to_sql('varieties', conn, if_exists='replace', index=False)
conn.close()


print("Los 4 reportes han sido guardados en sus respectivos formatos.")