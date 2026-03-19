import os
from collections.abc import Iterator, Mapping, Sequence
from dotenv import load_dotenv

ENV_LIST = (
	"DATABASE_HOST",
	"DATABASE_PORT",
	"DATABASE_USER",
	"DATABASE_PASSWORD",
	"DATABASE_SCHEMA",
	"DATABASE_DB_SOURCE",
	"DATABASE_DB_TARGET",
)

class EnvVariables(Mapping[str, str]):
	"""Read-only view of required environment variables."""

	def __init__(self, values: dict[str, str]) -> None:
		self._values = values

	@classmethod
	def from_environment(
		cls,
		required_keys: Sequence[str] = ENV_LIST,
		env_file: str = ".env",
	) -> "EnvVariables":
		load_dotenv(env_file)

		values: dict[str, str] = {}
		missing_keys: list[str] = []
		for key in required_keys:
			value = os.getenv(key)
			if value:
				values[key] = value
			else:
				missing_keys.append(key)

		if missing_keys:
			missing = ", ".join(missing_keys)
			raise ValueError(f"Missing required environment variables: {missing}")

		return cls(values)

	def __getitem__(self, key: str) -> str:
		return self._values[key]

	def __iter__(self) -> Iterator[str]:
		return iter(self._values)

	def __len__(self) -> int:
		return len(self._values)

	def __getattr__(self, item: str) -> str:
		try:
			return self._values[item]
		except KeyError as exc:
			raise AttributeError(item) from exc

	def as_dict(self) -> dict[str, str]:
		return dict(self._values)
