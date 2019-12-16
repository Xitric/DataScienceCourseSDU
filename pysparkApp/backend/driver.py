import re
import sys

module_name_regex = r"\/([\w_]+)-\d+(.\d+(.\d+)?)?-py3\.7\.egg$"
main_file_name = "__main__"


def get_module_name() -> str:
    for file in sys.path:
        match = re.search(module_name_regex, file)
        if match:
            return match.group(1)


def call_main():
    module = __import__(get_module_name(), fromlist=[main_file_name])
    main = getattr(module, main_file_name)
    main.run()


call_main()
