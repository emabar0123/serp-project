def filter_dictionary(dictionary, predicate=lambda k, v: True):
    if dictionary is not None:
        for k, v in dictionary.items():
            if predicate(k, v):
                yield k, v


def chunks(iterable, size, csv_size):
    """
    Splits an iterable object to chunks for iterating the list as chunks
    :param iterable: An iterable object
    :param size: Size of the chunks
    :return: Chunk size of the iterable object
    """
    if hasattr(iterable, "__len__"):
        return_list = []
        currnt_size = 0
        for i in iterable:
            currnt_size += len(i)
            return_list.append(i)
            if currnt_size > csv_size:
                yield return_list
                return_list = []
                currnt_size = 0
        yield return_list
    else:
        class ClassIterable:
            def __init__(self, generator, i_size, i_first):
                self.size = i_size
                self.generator = generator
                self.count = 0
                self.first = i_first
                self.iterme = self.make_read()

            def __iter__(self):
                return self.iterme

            def next(self):
                self.count += 1
                try:
                    return next(self.iterme)
                except StopIteration:
                    return

            def make_read(self):
                yield self.first
                for item in self.generator:
                    self.count += len(item)
                    if self.count > self.size:
                        return
                    yield item

        try:
            first = next(iterable)
        except StopIteration:
            return
        while first:
            yield ClassIterable(iterable, csv_size, first)
            try:
                first = next(iterable)
            except StopIteration:
                return