schema = {
    "name": "agora-datalab/water_stress",
    "description": """# Estrés Hídrico
Esta aplicación sirve para el cálculo de las estadisticas de imágenes satélite. El cálculo se hace sobre la zona de la imágen que está dentro de la geometría proporcionada.Esta herramienta pretende calcular las estadísticas de las imágenes de un sensor específico (satélite, avión, dron) tomadas en un corto periodo de tiempo para obtener unas gráficas y una geometría actualizada con los valores estadísticos. Puedes elegir entre hacer las estadícticas de tus propias imágenes o directamente obtener las estadísticas de imágenes de archivo procedentes de capturas de Sentinel-2 indicando la zona deseada y el índice espectral.
You can access the platform using the credentials generated for you by the platform administrator.
## Contact
If you have any questions, please contact us at [support@agora-datalab.eu](mailto:support@agora-datalab.eu).""",
    "labels": [
        "web-application",
        "data-service",
        "images",
        "water-stress",
    ],
    "jsonforms:schema": {
        "type": "object",
        "properties": {
            "username": {"type": "string", "readOnly": True},
            "password": {"type": "string", "readOnly": True},
        },
    },
    "jsonforms:uischema": {
        "type": "VerticalLayout",
        "elements": [
            {"type": "Label", "text": "Credentials"},
            {"type": "Control", "scope": "#/properties/username", "label": "Username"},
            {"type": "Control", "scope": "#/properties/password", "label": "Password"},
        ],
    },
    "jsonforms:data": {"username": "", "password": ""},
    "embed": "https://khaos.uma.es/water-stress/",
}
