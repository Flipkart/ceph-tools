from Util import Util
if __name__ == '__main__':
    start_time = Util.get_timestamp()
    dict = {}
    for i in range(1, 213940138 + 50):
        dict[i] = 1

    print Util.get_lapsed_time(start_time)