import argparse
import importlib.util
import os
import sys
from .microservice_handler import MicroserviceHandler


def main():
    parser = argparse.ArgumentParser(description='Run a microservice using the phoenix framework.')
    parser.add_argument('--path', help="Path that the phoenix need run from")
    parser.add_argument('microservice_module', help='The Python module name containing the microservice class.')
    parser.add_argument('microservice_class', help='The name of the microservice class.')
    args = parser.parse_args()

    # Import the microservice module relative to the current working directory
    path = os.getcwd()
    if args.path:
        try:
            path = args.path
            sys.path.append(args.path)

        except Exception:
            raise
    module_path = os.path.join(path, args.microservice_module + '.py')
    spec = importlib.util.spec_from_file_location(args.microservice_module, module_path)
    microservice_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(microservice_module)
    microservice_class = getattr(microservice_module, args.microservice_class)
    microservice_handler = MicroserviceHandler(
        microservice_class=microservice_class,
        logger_project_path=module_path
    )
    microservice_handler.phoenix_main()


if __name__ == "__main__":
    main()
