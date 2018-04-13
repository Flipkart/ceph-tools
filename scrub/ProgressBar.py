import progressbar as pb


class ProgressBar(object):
    def __init__(self, max_val):
        widgets = ['Helloworld: ', pb.Percentage(), ' ',
                   pb.Bar(marker="#"), ' ', pb.ETA()]
        self.__progress_bar = pb.ProgressBar(widgets=widgets, maxval=max_val)

    def _get_progress_bar(self):
        return self.__progress_bar

    def start(self):
        self._get_progress_bar().start()

    def update(self, items_processed):
        self._get_progress_bar().update(items_processed)

    def finish(self):
        self._get_progress_bar().finish()
