import re

PHONE_PATTERN_BR = re.compile(r"^\(\d{2}\)\s\d{4,5}-\d{4}$")
POSTAL_CODE_PATTERN_BR = re.compile(r"^\d{5}-\d{3}$")


def format_phone_br(value: str) -> str:
    digits = re.sub(r"\D", "", str(value))

    if len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    if len(digits) == 11:
        return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"

    return str(value).strip()
