from mrjob.job import MRJob, MRStep
import pandas as pd
import matplotlib.pyplot as plt


class LeastSoldCoffee(MRJob):

    def steps(self):
        return [MRStep(mapper= self.mapper_1, reducer= self.reducer_1),
                MRStep(mapper= self.mapper_2, reducer= self.reducer_2)]
    
    def mapper_1(self, _, line):
        transaction = line.split("\t")
        yield transaction[0], float(transaction[1])

    def reducer_1(self, coffee_type, cost):
        yield coffee_type, sum(cost)

    def mapper_2(self, coffee_type, summed_cost):
        yield None, (coffee_type, summed_cost)

    def reducer_2(self, _, coffee_info):
        pairs = list(coffee_info)
        sorted_pairs = sorted(pairs, key=lambda p: p[1], reverse=False)

        for word, count in sorted_pairs[:3]:
            yield word, count


def graph():
    data = plt




if __name__ == "__main__":
    LeastSoldCoffee.run()
    graph()


