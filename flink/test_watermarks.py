
# ============================================================
# PRUEBA LOCAL - EVENT TIME Y WATERMARKS
# PRE-ENTREGA 04 - DATA ENGINEERING
# ============================================================

from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.window import Time


# ============================================================
# CONVERSION DE FECHAS A MILISEGUNDOS UTC
# ============================================================

def to_milliseconds(hora):
    fecha = datetime.fromisoformat(
        f"2026-11-11T{hora}+00:00"
    )
    return int(fecha.timestamp() * 1000)


# ============================================================
# ASIGNACION DEL EVENT TIME
# ============================================================

class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        # La primera posicion contiene el timestamp UTC.
        return value[0]


# ============================================================
# PROCESAMIENTO
# ============================================================

def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Emitir watermarks con mayor frecuencia durante la prueba.
    env.get_config().set_auto_watermark_interval(100)

    # Eventos deliberadamente desordenados.
    eventos = [
        (to_milliseconds("12:30:05"), "compra", 1, 1000.0),
        (to_milliseconds("12:30:20"), "compra", 1, 2000.0),
        (to_milliseconds("12:30:12"), "compra", 1, 500.0),
        (to_milliseconds("12:31:15"), "compra", 1, 3000.0),
    ]

    tipo_evento = Types.TUPLE([
        Types.LONG(),
        Types.STRING(),
        Types.INT(),
        Types.DOUBLE()
    ])

    stream = env.from_collection(
        collection=eventos,
        type_info=tipo_evento
    )

    # Watermark con tolerancia de 10 segundos.
    estrategia = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    stream_con_tiempo = stream.assign_timestamps_and_watermarks(
        estrategia
    )

    # Agrupacion por tipo de operacion.
    # Flink conserva acumuladores por clave y ventana.
    resultados = (
        stream_con_tiempo
        .key_by(lambda evento: evento[1], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(60)))
        .reduce(
            lambda a, b: (
                a[0],
                a[1],
                a[2] + b[2],
                a[3] + b[3]
            ),
            output_type=tipo_evento
        )
    )

    # Mostrar agregaciones procesadas.
    resultados.print()

    env.execute("prueba-watermarks-preentrega04")


if __name__ == "__main__":
    main()
