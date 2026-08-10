"""
T6 — Result Divergence.

Braucht eine Rust-Injection die den serialisierten Result vor dem Send
korrumpiert. Der Payload-Typ ist generisch (`C::Payload`), sodass eine
generische Bit-Flip-Injection ohne zusaetzlichen Trait-Bound nicht sauber
machbar ist. Optionen fuer spaeter:

  a) Trait `TestableCorruption` fuer den konkreten Voter-Payload, das
     eine `corrupt(&mut self)` Methode bereitstellt. Dann eine neue
     Injection `InjectCorruptResult { count }`.
  b) Test-only Feature-Gate mit einer konkreten Impl fuer ETCS-Payload.

Bis dahin: skip. Der Codepfad wird durch T5 (Input-Divergenz)
weitgehend abgedeckt, weil beide durch Divergence-Gates laufen.
"""
import pytest

pytestmark = pytest.mark.skip(
    reason="Result-Corruption braucht generischen Payload-Trait — future work"
)


def test_result_divergence(fabric_3):
    pass
