from mrjob.job import MRJob


class CoffeeSalesSum(MRJob):
    def mapper(self, _, line):
        transaction = line.split("\t")
        yield transaction[0], float(transaction[2])

    def reducer(self, name, cost):
        yield name, round(sum(cost), 2)


if __name__ == "__main__":
    CoffeeSalesSum.run()