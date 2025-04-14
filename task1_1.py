from pymongo import MongoClient


def main() :
    client = MongoClient("mongodb://localhost:27017/")
    database = client["comp3210_a1"]
    sales = database["coffee_sales"]
    new_collection = database["coffee_extracted"]

    with open("task_1_1_output.txt", "w") as file:
        for transaction in sales.find():
            #skipping transactions without a card ID
            card = transaction["card"]
            if card == None:
                continue

            # Creating Triplet
            lst = [None] * 7
            lst[0] = transaction["coffee_name"]
            lst[1] = card[-4:]
            lst[2] = transaction["money"]
            lst[3] = transaction["date"].day
            lst[4] = transaction["datetime"].month
            lst[5] = transaction["datetime"].year
            lst[6] = transaction["datetime"].strftime("%H:%M:%S")


            # Writing Information to file
            line = ""
            for field in lst:
                line += str(field) + "\t"

            #Subscripting the line because as I added a tab after the last entry
            file.write(line[:-1] + "\n")


            #Adding the transaction detail to the new mongo collection
            fields = ["coffee_type", "user_id", "money", "day", "month", "year", "time"]
            row_dictionary = dict(zip(fields, lst))

            new_collection.insert_one(row_dictionary)
            

if __name__ == "__main__":
    main()