install:
	pip install --upgrade pip && pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:
	black *.py 

lint:
	# Disable comment to test speed
	# pylint --disable=R,C --ignore-patterns=test_.*?py *.py mylib/*.py
	# Ruff linting is 10-100X faster than pylint
	ruff check *.py mylib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

refactor: format lint

deploy:
	# Deploy goes here

all: install lint test format deploy

job:
	python run_job.py

generate_and_push:
	# Run the Python script to generate visualizations
	python mylib/query_viz.py || { echo "Error: mylib/query_viz.py failed. Exiting."; exit 1; }

	# Check if any changes were made to the visualizations folder or other files
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "Changes detected. Preparing to commit and push..."; \
		git config --local user.email "action@github.com"; \
		git config --local user.name "GitHub Action"; \
		git add visualizations/; \
		git commit -m "Automated update: Add generated visualizations"; \
		git push || { echo "Error: Failed to push changes. Exiting."; exit 1; }; \
	else \
		echo "No changes detected in visualizations folder. Skipping commit and push."; \
	fi

