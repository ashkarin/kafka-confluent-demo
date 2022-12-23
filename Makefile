init:
	@echo "Starting infrastructure"
	@docker compose up -d

install:
	@poetry install

consume:
	@poetry run python src/consume-messages.py

produce:
	@poetry run python src/produce-messages.py

register:
	@poetry run python src/register-schemas.py