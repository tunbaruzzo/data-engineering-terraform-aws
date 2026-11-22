
# ============================================================
# TESTS DEL PRODUCTOR FLINK
# PRE-ENTREGA 05 - DATA ENGINEERING
# ============================================================

import json
import sys
import unittest

from datetime import datetime
from pathlib import Path


# Permitir importar el productor desde scripts/.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from producer_flink import generate_event, validate_event


class TestProducerFlink(unittest.TestCase):

    def setUp(self):
        self.event = generate_event()

    def test_required_fields(self):
        expected = {
            "transaction_id",
            "user_id",
            "event_type",
            "amount",
            "currency",
            "timestamp"
        }

        self.assertEqual(
            set(self.event.keys()),
            expected
        )

    def test_field_types(self):
        self.assertIsInstance(
            self.event["transaction_id"], str
        )
        self.assertIsInstance(
            self.event["user_id"], str
        )
        self.assertIsInstance(
            self.event["event_type"], str
        )
        self.assertIsInstance(
            self.event["amount"], (int, float)
        )
        self.assertIsInstance(
            self.event["currency"], str
        )
        self.assertIsInstance(
            self.event["timestamp"], str
        )

    def test_business_values(self):
        self.assertIn(
            self.event["event_type"],
            {"compra", "pago", "devolucion"}
        )

        self.assertGreaterEqual(
            self.event["amount"], 100
        )

        self.assertLessEqual(
            self.event["amount"], 50000
        )

        self.assertEqual(
            self.event["currency"], "ARS"
        )

    def test_timestamp_utc(self):
        timestamp = datetime.fromisoformat(
            self.event["timestamp"]
        )

        self.assertIsNotNone(timestamp.tzinfo)
        self.assertEqual(
            timestamp.utcoffset().total_seconds(),
            0
        )

    def test_json_serialization(self):
        encoded = json.dumps(self.event)
        decoded = json.loads(encoded)

        self.assertEqual(decoded, self.event)


    # ========================================================
    # TESTS DE VALIDACION - EVENTOS VALIDOS E INVALIDOS
    # ========================================================

    def test_validate_valid_event(self):
        self.assertTrue(validate_event(self.event))

    def test_validate_missing_field(self):
        event = self.event.copy()
        del event["transaction_id"]

        self.assertFalse(validate_event(event))

    def test_validate_invalid_amount(self):
        for amount in [-100, 0, 60000, "1500", True]:
            with self.subTest(amount=amount):
                event = self.event.copy()
                event["amount"] = amount

                self.assertFalse(validate_event(event))

    def test_validate_invalid_timestamp(self):
        for timestamp in [
            "fecha-invalida",
            "2026-11-20T12:00:00",
            "2026-11-20T12:00:00-03:00"
        ]:
            with self.subTest(timestamp=timestamp):
                event = self.event.copy()
                event["timestamp"] = timestamp

                self.assertFalse(validate_event(event))

    def test_validate_invalid_event_type(self):
        event = self.event.copy()
        event["event_type"] = "tipo-desconocido"

        self.assertFalse(validate_event(event))


if __name__ == "__main__":
    unittest.main()
