# Propuesta de Valor — Casos de Uso para Clientes

> El cliente no compra tecnología. Compra que le dejen de doler las cosas.

Este documento describe 10 problemáticas reales de empresas medianas que el stack DuckLake puede resolver. Están redactadas desde la perspectiva del cliente, no de la tecnología.

---

## 1. "Tengo 5 sistemas y no puedo cruzar la info"

**El problema más común en empresas de 50–500 personas.**

ERP, CRM, plataforma de ventas, logística, contabilidad — cada uno en su silo, nadie habla con nadie. El CFO cierra el mes con 3 versiones distintas del mismo número en 3 Excel diferentes y no sabe cuál creer.

**Lo que resolvemos:** Conectamos todas las fuentes, unificamos, y generamos UNA versión de la verdad. El cierre mensual pasa de 2 semanas a 2 horas.

**Interlocutores clave:** CFO · Controller · Gerente de Operaciones

---

## 2. "Me enteré que perdí un cliente cuando ya se fue"

**El churn invisible.**

Las empresas saben que pierden clientes. No saben cuándo van a perderlos. Ven la caída en el reporte mensual, cuando ya es tarde para hacer algo.

**Lo que resolvemos:** Con el historial de transacciones construimos señales de alerta temprana — frecuencia de compra cayendo, ticket bajando, inactividad creciente — y generamos una alerta accionable antes de que el cliente se vaya.

**Interlocutores clave:** Director Comercial · Gerente de Cuentas · Marketing

---

## 3. "No sé cuál de mis productos, sucursales o vendedores realmente me gana plata"

**La rentabilidad invisible.**

El reporte dice que vendieron $10M. Pero nadie sabe cuánto costó vender cada cosa, qué margen dejó cada canal, qué sucursal está comiendo la ganancia de las otras. Revenue y rentabilidad son cosas distintas y muy pocas empresas tienen acceso a la segunda.

**Lo que resolvemos:** Cruzamos ventas con costos, logística, devoluciones y descuentos. Mostramos rentabilidad real por producto, por zona, por vendedor. El gerente toma decisiones que antes tomaba a ojo.

**Interlocutores clave:** CEO · CFO · Gerente Comercial

---

## 4. "Mis proveedores me mandan archivos y los proceso a mano"

**El Excel hell operativo.**

Distribuidoras, importadoras, empresas con muchos proveedores o sucursales: reciben decenas de archivos por semana (CSV, Excel, PDF), los procesan a mano, alguien los valida, alguien los carga. Es lento, caro y lleno de errores humanos.

**Lo que resolvemos:** Automatizamos la ingesta. Cualquier formato que manden los proveedores entra al pipeline, se valida, se normaliza y llega limpio al sistema. El operador que pasaba datos 4 horas por día puede hacer otra cosa.

**Interlocutores clave:** Gerente de Compras · Operaciones · Logística

---

## 5. "Quiero hacer IA pero mis datos son un desastre"

**La promesa de IA vs. la realidad.**

Toda empresa mediana-grande quiere usar inteligencia artificial — chatbots internos, recomendaciones, automatización. Contratan a alguien para hacerlo y el proyecto muere en la fase de datos: están sucios, dispersos, sin formato, con información sensible mezclada y sin documentación.

**Lo que resolvemos:** Construimos la base de datos que alimenta la IA — limpia, estructurada, versionada, con trazabilidad. El proyecto de IA del cliente finalmente puede arrancar porque tiene datos dignos.

**Interlocutores clave:** CTO · Director de Innovación · cualquier empresa que ya intentó hacer algo con IA y fracasó

---

## 6. "Pagamos una fortuna en herramientas de datos que nadie usa"

**El Snowflake/Databricks remordimiento.**

Empresas medianas que se comieron el marketing de las big tech: compraron soluciones enterprise caras. Usan el 15% de las features, pagan el 100% del costo, y los reportes igual los siguen haciendo en Excel porque nadie aprendió a usar la herramienta.

**Lo que resolvemos:** Migramos a un stack austero y eficiente. Mismo resultado, fracción del costo. El dinero que se libera puede reinvertirse o es ganancia directa en el P&L de IT.

**Interlocutores clave:** CFO · CTO · cualquier gerente que tiene que justificar el presupuesto de tecnología

---

## 7. "No puedo planificar el inventario y siempre me sobra o me falta"

**El problema de demanda de cualquier empresa con productos físicos.**

Sobrestock es plata inmovilizada. Rotura de stock es venta perdida. Ambos son evitables con datos. El problema es que la mayoría planifica mirando el mes pasado en vez del mes que viene.

**Lo que resolvemos:** Con el histórico de ventas construimos modelos de demanda por producto, por zona, por temporada. El área de compras y logística planifica con números, no con intuición.

**Interlocutores clave:** Gerente de Compras · Logística · Supply Chain

---

## 8. "No sé si el dato que me muestran es correcto"

**La desconfianza en el dato propio.**

En muchas empresas nadie confía en los reportes. Cada vez que sale un número, alguien dice "pero eso no está bien porque...". La consecuencia es que nadie toma decisiones con datos — las toman con intuición igual, pero pagan el sistema de todas formas.

**Lo que resolvemos:** Implementamos data quality como parte del pipeline — cada tabla tiene validaciones, cada anomalía se detecta antes de que llegue al reporte, cada número tiene trazabilidad. El dato llega con sello de calidad.

**Interlocutores clave:** Todo el management. Este es el problema que hace que los proyectos de datos fracasen.

---

## 9. "Tenemos datos que no podemos compartir externamente pero necesitamos hacerlo"

**Compliance + colaboración.**

Empresas que quieren compartir data con un proveedor, un auditor, un socio o un equipo externo — pero no pueden porque contiene información sensible: clientes, precios, operaciones confidenciales.

**Lo que resolvemos:** Generamos una versión anonimizada o sintética de los datos — estadísticamente equivalente a la original, sin información sensible, auditable. El cliente puede colaborar sin riesgo regulatorio ni reputacional.

**Interlocutores clave:** Legal/Compliance · CTO · empresas en industrias reguladas (salud, finanzas, gobierno)

---

## 10. "Cada reporte lo hace una persona distinta y cada uno sale diferente"

**La inconsistencia organizacional.**

La métrica "tasa de conversión" la calculan distinto en comercial, en marketing y en operaciones. Nadie sabe quién tiene razón. Los gerentes van a la reunión con números distintos y pierden 45 minutos discutiendo el Excel en vez de tomar decisiones.

**Lo que resolvemos:** Centralizamos las definiciones de negocio en el pipeline de datos. Cada métrica se calcula una sola vez, de una sola manera, y todos consumen el mismo número. Las reuniones pasan de "¿cuál es el número correcto?" a "¿qué hacemos con este número?".

**Interlocutores clave:** CEO · CFO · cualquier empresa en modo de crecimiento donde los equipos empezaron a desalinearse

---

## Cómo usar este documento en una venta

No vayas con *"tenemos un data lake en DuckDB"*. Ve con preguntas:

> *"¿Cuánto tiempo tarda tu equipo en cerrar el mes?"*
> *"¿Confiás en los números que te muestran?"*
> *"¿Cuándo fue la última vez que tomaste una decisión importante con datos y no con intuición?"*

Con dos de esas tres preguntas respondidas con incomodidad, ya tenés una venta en proceso.

---

## Stack detrás de la solución

| Capa | Tecnología | Rol |
|------|-----------|-----|
| Ingesta | Python connectors | Cualquier fuente: SFTP, API, base de datos, archivos |
| Almacenamiento | Parquet + DuckDB | Barato, rápido, sin servidor |
| Transformación | dbt | SQL puro, testeado, documentado |
| Orquestación | Dagster | Pipeline automatizado, observable |
| Destino | PostgreSQL | Cualquier herramienta de BI lo consume |
| Deploy | Railway / cualquier cloud | Sin gestión de infraestructura |

El cliente no gestiona nada de esto. Nosotros lo entregamos funcionando.
