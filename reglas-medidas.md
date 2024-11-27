#Reglas para medidas

Quiero extraer las medidas de los títulos de los productos (columna "DESCRIPCION") y siguiendo las siguientes reglas.

Las valores a extraer pueden ser: 
- Ancho: metafield "ancho"
- Alto: metafield "alto"
- Largo: metafield "largo"
- Grosor: metafield "grosor"
- Diametro: metafield "diametro"
- medidas: megafield "medidas"
- medidas zona del grabado: megafield "medidas_zona_grabado"
- medidas chatón: megafield "medidas_chaton"

Otras reglas
- Si se encuentra un patrón tipo numeroxnumero, se extraen los valores y se crea un metafield "medidas" con el valor anchoxalto. el primer número es el alto y el segundo el ancho
- El ancho pueden venir de diversas formas: "ANCHO: 22MM", "ANCHO 22MM", "ANCHO: 22 MM", "ANCHO 22 MM"
- El alto pueden venir de diversas formas: "ALTO: 22MM", "ALTO 22MM", "ALTO: 22 MM", "ALTO 22 MM"
- El Largo pueden venir de diversas formas: "LARGO: 22CM", "LARGO 22CM", "LARGO: 22 CM", "LARGO 22 CM", si vienen en MM descartalo
- El largo también puede estar indicado como "Longitud", en ese caso se asigna a Largo

Reglas Por tipos de producto
- Si el producto es de tipo Alianza, solitario, sortija
-- si viene una sola medida en milímetros se asigna al ancho
- Si el productos es un **sello**
    -- Si viene una medida en formato altoxancho por defecto se asigna a "medidas_chaton"
    -- Si esa medida viene precedida de la palabra "grabado", entonces adjudicadmos la medida a "medidas_zona_grabado"
- Si el producto es de tipo **esclava** o **pulsera**:
    -- si viene una sola medida en milímetros se asigna al grosor
    -- si viene una medida en CM siempre se asigna al largo
- Si el tipo de producto es **AROS**, **MEDALLA**, **ESCAPUARIO** o **COLGANTE**
  - Si viene una medida sola, en milimetros, sin indicar antes el tipo de medida, lo asignamos a Diametro
- Si el producto es de tipo **CADENA** o **COLLAR**
  - Si se indica un valor en CM sin indicar antes el tipo de medida se asigna a "largo"
  - Si se indica un valor en MM sin indicar antes el tipo de medida, lo asignamos a Ancho

- Si el producto es un colgante y viene una sola medida en milímetros sin indicar si es ancho o alto, se asigna a diametro


#Reglas para calidades de los brillantes

Quiero extraer de los títulos la calidad del diamante que incluye la joya, seguiremos estas reglas
- En el titulo del producto tiene que aparecer "Brillante" o "Diamante"
- Metafields que usaremos
  -- Kilates del diamante: "kilates_diamante"
    --- se detecta porque es una medida numérica (decimal o entero) que antecede a la cadena "QTS" (ej: 18K SOLITARIO ORO AMARILLO DIAMANTE TALLA BRILLANTE 0.10 QTS. COLOR H VSI la calidad sería 0.10)
  -- Color del diamante: "color_diamante"
    --- puede tener los siguientes valores: G, H o I
    --- puede venir puede preceder a la palabra "COLOR"
    --- puede ir combinado mediante guiones, antes o después, con el valor de pureza (FL, IF, VVS1, VVS2, VS1, VS2, SI1, SI2, I1, I2, I3)
    

#Reglas para extraer piedras
- Extraer las piedras preciosas o semipreciosas que contenta el título
- El producto puede tener varias piedras