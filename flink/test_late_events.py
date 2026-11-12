
# ============================================================
# PRUEBA DETERMINISTA - EVENTOS TARDIOS Y WATERMARKS
# PRE-ENTREGA 04 - DATA ENGINEERING
#
# Simula el comportamiento temporal de Flink.
# No ejecuta el motor Apache Flink ni utiliza AWS.
# ============================================================

from datetime import datetime, timezone, timedelta
from collections import defaultdict


# ============================================================
# CONFIGURACION
# ============================================================

VENTANA_SEGUNDOS = 60
TOLERANCIA_SEGUNDOS = 10


def fecha(hora):
    return datetime.fromisoformat(
        f"2026-11-11T{hora}+00:00"
    )


def formato(valor):
    return valor.strftime("%H:%M:%S.%f")[:-3]


# ============================================================
# EVENTOS SIMULADOS - ORDEN DE LLEGADA
# ============================================================

eventos = [
    ("12:30:05", "compra", 1000.0),
    ("12:30:20", "compra", 2000.0),
    ("12:30:12", "compra", 500.0),
    ("12:31:15", "compra", 3000.0),
    ("12:30:08", "compra", 800.0),
]


# ============================================================
# SIMULACION DEL PROCESAMIENTO
# ============================================================

def main():

    max_event_time = None
    watermark = datetime.min.replace(tzinfo=timezone.utc)

    ventanas = defaultdict(
        lambda: {"cantidad": 0, "total": 0.0}
    )

    cerradas = {}
    descartados = []

    print("\nINICIANDO SIMULACION DE WATERMARKS\n")

    for hora, tipo, importe in eventos:

        event_time = fecha(hora)

        # Calcular el inicio y fin de la ventana.
        inicio = event_time.replace(second=0, microsecond=0)
        fin = inicio + timedelta(seconds=VENTANA_SEGUNDOS)

        clave = (inicio, tipo)

        # Evaluar contra el watermark ANTERIOR al evento.
        if watermark >= fin:
            descartados.append((hora, tipo, importe))

            print(
                f"TARDIO: {hora} | "
                f"{tipo} | ${importe:.2f}"
            )
            continue

        # Incorporar el evento al acumulador.
        ventanas[clave]["cantidad"] += 1
        ventanas[clave]["total"] += importe

        print(
            f"ACEPTADO: {hora} | "
            f"{tipo} | ${importe:.2f}"
        )

        # Actualizar el maximo Event Time observado.
        if max_event_time is None or event_time > max_event_time:
            max_event_time = event_time

        # Watermark simulado:
        # max_event_time - tolerancia - 1 milisegundo.
        nuevo_watermark = (
            max_event_time
            - timedelta(seconds=TOLERANCIA_SEGUNDOS)
            - timedelta(milliseconds=1)
        )

        if nuevo_watermark > watermark:
            watermark = nuevo_watermark

        # Cerrar ventanas cuyo fin ya fue superado.
        for clave_ventana in list(ventanas):

            inicio_ventana, tipo_ventana = clave_ventana
            fin_ventana = inicio_ventana + timedelta(
                seconds=VENTANA_SEGUNDOS
            )

            if watermark >= fin_ventana:
                cerradas[clave_ventana] = ventanas.pop(
                    clave_ventana
                )

                print(
                    f"  -> Ventana cerrada: "
                    f"{formato(inicio_ventana)} - "
                    f"{formato(fin_ventana)}"
                )

        print(f"  Watermark: {formato(watermark)}")

    # Simular el watermark final de una fuente finita.
    cerradas.update(ventanas)

    print("\nRESULTADOS FINALES")
    print("-" * 55)

    for (inicio, tipo), datos in sorted(cerradas.items()):
        promedio = datos["total"] / datos["cantidad"]

        print(
            f"{formato(inicio)} | {tipo} | "
            f"Cantidad: {datos['cantidad']} | "
            f"Total: ${datos['total']:.2f} | "
            f"Promedio: ${promedio:.2f}"
        )

    print(f"\nEventos tardios descartados: {len(descartados)}")

    # Validaciones automaticas.
    primera_ventana = (
        fecha("12:30:00"), "compra"
    )

    assert cerradas[primera_ventana]["cantidad"] == 3
    assert cerradas[primera_ventana]["total"] == 3500.0
    assert len(descartados) == 1
    assert descartados[0][0] == "12:30:08"

    print("\nPRUEBA DETERMINISTA SUPERADA")


if __name__ == "__main__":
    main()
