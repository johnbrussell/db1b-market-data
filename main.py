import sys


class DB1B:
    def __init__(self, output_path, input_paths):
        assert output_path.endswith('.csv')
        self._output_path = output_path
        assert len(input_paths) > 0
        self._input_paths = input_paths


def main():
    db1b = DB1B(sys.argv[1], sys.argv[2:])


if __name__ == '__main__':
    main()
