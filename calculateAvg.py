import multiprocessing
import concurrent.futures


def generate_line(file, num):
    """Return a generator that yields the next <num> number of lines as a list from <file>"""
    with open(file) as f:
        while True:
            line = [f.readline().strip() for _ in range(num)]
            if not any(line):
                break
            yield line


def process_line(line, output):
    """Take a line (data point) of the type station;temp, calculate average min, max if station
       is present in the output dictionary. Create the station key and corresponding value if not
    """
    # remove newline and convert it to float
    _line = line[:-1:]
    _line = _line.split(";")
    _station = _line[0]
    _temp = float(_line[1])
    if _station in output:
        # the data structure would look like _station: (sum, num, min, max, avg)
        _sum = output[_station][0] + _temp
        _num = output[_station][1] + 1
        _min = output[_station][2]
        _max = output[_station][3]
        if _min > _temp:
            _min = _temp
        if _max < _temp:
            _max = _temp
        _avg = _sum / _num
        output[_station] = (_sum, _num, _min, _max, _avg)
    else:
        # add _station to the dictionary
        output[_station] = (_temp, 1, _temp, _temp)

def process(lines, output):
    """Take a list of lines <lines> and process it"""
    for line in lines:
        process_line(line, output)
    return


if __name__ == "__main__":
    output = multiprocessing.Manager().dict()
    line_generator = generate_line("measurements.txt", 100000)
    processes = []
    with concurrent.futures.ProcessPoolExecutor(5) as executor:
        result = [executor.submit(process, line, output) for line in line_generator]

    output = sorted(output.items(), key=lambda x: x[0])
    print(output)
