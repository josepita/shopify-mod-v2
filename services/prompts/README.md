Guía de plantillas de prompts para descripciones IA

Estructura
- `prompt_armaan.txt`: plantilla base del prompt (parametrizable con llaves `{{ ... }}`).
- `mapping.yaml`: mapeo de campos del catálogo/Shopify a variables de la plantilla.

Cómo usar tu prompt existente
1) Copia tu prompt al archivo de la plantilla:
   - En WSL/Linux:
     `cp "/mnt/c/Users/josepita/OneDrive - Trevenque Group/Escritorio/prompt-armaan.txt" services/prompts/prompt_armaan.txt`
   - O pega manualmente su contenido en `services/prompts/prompt_armaan.txt`.
2) Ajusta `mapping.yaml` para que las variables coincidan con tus columnas/atributos.

Variables disponibles (sugeridas)
- `{{ titulo }}`: título del producto.
- `{{ categoria }}`, `{{ subcategoria }}`: taxonomía.
- `{{ marca }}`: marca (si existe en datos).
- `{{ bullets_specs }}`: bullets con especificaciones técnicas (ya preformateado).
- `{{ beneficios_bullets }}`: bullets de beneficios (si aplican).
- `{{ medidas_reglas }}`: reglas/ayudas de medidas (opcional, desde `reglas-medidas.md`).
- `{{ tono }}`: ajuste de tono (breve, claro, SEO-friendly).

Notas
- La plantilla debe pedir salida en HTML controlado (p, ul, ol, li, br, strong, em) y en español.
- Evitar invenciones: instruye explícitamente a omitir datos ausentes.
- El módulo de generación completará `bullets_specs` a partir de campos del CSV/Shopify según `mapping.yaml`.

