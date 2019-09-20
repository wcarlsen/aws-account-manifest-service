install:
	pipenv install

install_dev:
	pipenv install --dev

run:
	pipenv run python src/service.py

deps:
	docker-compose up --build

test:
	pipenv run flake8 src/ --max-line-length=88
	pipenv run pylint src/ --max-line-length=88 --disable=W,C,R
	pipenv run mypy src/ --ignore-missing-imports
	pipenv run pytest