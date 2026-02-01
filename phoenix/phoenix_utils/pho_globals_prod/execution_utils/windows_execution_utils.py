import subprocess


class Execution:
    def __init__(self):
        return

    @staticmethod
    def execute_command_pipe_with_errors(command_arguments):
        output = None
        errors = None
        return_code = None
        process = subprocess.Popen(args=command_arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
        if process is not None:
            output, errors = process.communicate()
            return_code = process.returncode
        return output, errors, return_code

    @staticmethod
    def execute_command_call(command_arguments):
        print(" ".join(command_arguments))
        output = subprocess.call(args=command_arguments)
        return output

    @staticmethod
    def execute_command_check_output(command_arguments):
        print(" ".join(command_arguments))
        output = subprocess.check_output(args=command_arguments)
        return output