# BPO Voice Report Automation

Pipeline de procesamiento de datos en Python que transforma archivos .csv crudos en un dataset limpio y estructurado
para su visualización en Power BI

## Tecnologias y librerias usadas

- Python 3.x
- Pandas
- NumPy
- Power BI
- python-dotenv

## 📁 Estructura del proyecto
```
Automation_voice/
├── images
│   └── dashboard.png
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
├── voice-2.0.py
```
## 🚀 Instalación

1. Clona el repositorio
```
git clone https://github.com/tu-usuario/tu-repositorio.git
```

2. Instala las dependencias
```
pip install -r requirements.txt
```

3. Crea tu archivo `.env` basándote en `.env.example` y completa tus rutas locales

4. Corre el script
```
python Automatizaciones_Intervalos/Voice.py
```

## 📊 Dashboard Power BI

Vista general del reporte de voz con métricas clave de call center:
Service Level, AHT, ASA, Abandon Rate y volumen por intervalo.

![Dashboard](Reporting_Automations\Images\Dashboard.png)