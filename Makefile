.PHONY: conda-update
conda-update:
	envsubst < environment.yaml > /tmp/environment.yaml
	conda env update -n dask-saturn --file /tmp/environment.yaml

.PHONY: format
format:
	black --line-length 100 .

.PHONY: lint
lint:
	flake8 --count --max-line-length 100 --exclude versioneer.py .
	black --check --diff --line-length 100 .
	# pylint disables:
	#   * C0301: line too long
	#   * C0103: snake-case naming
	#   * C0330: wrong hanging indent before block
	#   * E0401: unable to import
	#   * R0903: too few public methods
	#   * R0913: too many arguments
	#   * R0914: too many local variables
	#   * R1705: Unnecessary "else" after "return"
	#   * R1720: Unnecessary "elif" after "return"
	#   * W0107: unnecessary "pass" statement
	#   * W0212: access to protected member
	#   * W0221: parameters differ from overridden method
	#   * W1203: use lazy % formatting
	pylint --disable=C0103,C0301,C0330,E0401,R0903,R0913,R0914,R1705,R1720,W0107,W0212,W0221,W1203 dask_saturn/
