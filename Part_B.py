import os
import math
import csv
import json
import hashlib

DATASETS_PATH = './datasets/'
REPORT_USERS_FILE = './user-counter.csv'
REPORT_TAG_FILE = './tag-counter.csv'
MAX_COUNTS_LIMIT = 5

class Stream:
    def __init__(self):
        self.filenames = self.get_json_files(DATASETS_PATH)
        self.lines = []
        self.read_next_file()

    def get_json_files(self, path):
         return os.listdir(path)

    def get_next_batch(self, batch_size = 1000):
        lines_to_return = self.lines[:batch_size]
        del self.lines[:batch_size]
        if len(self.lines) == 0:
            self.read_next_file()

        return lines_to_return

    def read_next_file(self):
        if len(self.filenames) == 0:
            return []

        filename = self.filenames.pop()
        print("Reading next file:", filename)
        with open(DATASETS_PATH + filename) as f:
            self.lines = list(map(lambda l: json.loads(l), f.readlines()))

class HyperLogLog:
    # Works for up to 64 buckets
    def __init__(self, buckets):
        self.buckets = [0] * buckets

    def update(self, value):
        binary = self.hash_to_fixed_binary(str(value))
        bucket_idx, zeroes_count = self.parse_hash(binary)
        self.buckets[bucket_idx] = max(self.buckets[bucket_idx], zeroes_count)

    def get_unique_count(self):
        transformed = []
        for i in self.buckets:
            transformed.append(2 ** -i)

        return (len(self.buckets) ** 2) * (sum(transformed) ** -1)

    def parse_hash(self, h):
        bucket_idx = int(h[0:16], 2) % len(self.buckets)
        zeroes_count = h[16:].index('1')
        return bucket_idx, zeroes_count

    def hash_to_fixed_binary(self, value):
        md5 = hashlib.md5()
        md5.update(value.encode())
        hexcode = md5.hexdigest()
        binary = bin(int(hexcode, 16))
        return str(binary)

class MinCountSketch:
    def __init__(self, width, height):
        self.width = width
        self.height = height
        self.table = [[0] * height for _ in range(width)]

    def increment_count(self, value):
        for i in range(0, self.height):
            self.table[self.hash_for_column(value, i)][i] += 1

    def get_count(self, value):
        min_cnt = math.inf

        for i in range(0, self.height):
            idx = self.hash_for_column(value, i)
            if self.table[idx][i] < min_cnt:
                min_cnt = self.table[idx][i]

        return min_cnt

    def hash_for_column(self, value, col):
        return hash(str(value) + str(col)) % self.width

class WithFrequenciesCounter:
    def __init__(self, stream):
        self.stream = stream
        self.user_frequencies = {}
        self.tag_frequencies = {}

        self.run()

    def run(self):
        batch = self.stream.get_next_batch()
        while len(batch) > 0:
            for post in batch:
                user_id = post['user']['id']
                self.user_frequencies[user_id] = self.user_frequencies.get(user_id, 0) + 1

                for tag in post['entities']['hashtags']:
                    self.tag_frequencies[tag['text']] = self.tag_frequencies.get(tag['text'], 0) + 1

            self.report_most_active('tags', self.tag_frequencies)
            self.report_most_active('users', self.user_frequencies)
            batch = self.stream.get_next_batch()

    def report_most_active(self, entity, frequencies):
        sorted_freq_pairs = sorted(frequencies.items(), key=lambda item: item[1], reverse = True)
        print("Biggest frequencies up to this batch for", entity, ": " , sorted_freq_pairs[:MAX_COUNTS_LIMIT])

class WithMinCountCounter:
    def __init__(self, stream):
        self.stream = stream
        self.user_counter = MinCountSketch(1000, 1000)
        self.tag_counter = MinCountSketch(1000, 1000)

        self.run()

    def run(self):
        batch = self.stream.get_next_batch()
        while len(batch) > 0:
            for post in batch:
                user_id = post['user']['id']
                self.user_counter.increment_count(user_id)

                for tag in post['entities']['hashtags']:
                    self.tag_counter.increment_count(tag['text'])

            batch = self.stream.get_next_batch()

class WithSetsUniqueCounter:
    def __init__(self, stream):
        self.stream = stream
        self.users = set()
        self.tags = set()

        self.run()

    def run(self):
        batch = self.stream.get_next_batch()
        while len(batch) > 0:
            for post in batch:
                user_id = post['user']['id']
                self.users.add(user_id)

                for tag in post['entities']['hashtags']:
                    self.tags.add(tag['text'])

            batch = self.stream.get_next_batch()

        print("Unique users", len(self.users))
        print("Unique tags", len(self.tags))

class WithHLLUniqueCounter:
    def __init__(self, stream):
        self.stream = stream
        self.users = HyperLogLog(50)
        self.tags = HyperLogLog(50)

        self.run()

    def run(self):
        batch = self.stream.get_next_batch()
        while len(batch) > 0:
            for post in batch:
                user_id = post['user']['id']
                self.users.update(user_id)

                for tag in post['entities']['hashtags']:
                    self.tags.update(tag['text'])

            print("Unique users", self.users.get_unique_count())

            batch = self.stream.get_next_batch()

        print("Unique users", len(self.users))
        print("Unique tags", len(self.tags))

def main():
    stream = Stream()
    # count_with_freq = WithFrequenciesCounter(stream)
    # count_with_min_counter = WithMinCountCounter(stream)
    # WithSetsUniqueCounter(stream)
    # WithHLLUniqueCounter(stream)

main()