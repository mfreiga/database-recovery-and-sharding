# sharding.py
import json
import pickledb


class ShardedDatabase:
    # Read books from JSON file

    def __init__(self):
        self.num_nodes = 10
        self.nodes = {i: pickledb.load(f"database_node_{i}.db", False) for i in range(0, 10)}
        self.store_books()

    def hash_key(self, book):
        # For this example, we determine the node based on the first letter of the key

        if not book[0].isalpha():
            return 9
        first_letter = book[0].upper()
        if 'A' <= first_letter <= 'C':
            return 0
        elif 'D' <= first_letter <= 'F':
            return 1
        elif 'G' <= first_letter <= 'I':
            return 2
        elif 'J' <= first_letter <= 'L':
            return 3
        elif 'M' <= first_letter <= 'O':
            return 4
        elif 'P' <= first_letter <= 'R':
            return 5
        elif 'S' <= first_letter <= 'U':
            return 6
        elif 'V' <= first_letter <= 'X':
            return 7
        elif 'Y' <= first_letter <= 'Z':
            return 8
        else:
            return 0

    def store_books(self):
        for book in books:
            # Map study courses to hash-modulo keys
            node_index = self.hash_key(book)
            self.nodes[node_index].set(book, node_index)
            self.nodes[node_index].dump()

    def check_if_book_exists(self, book_name):
        node_index = self.hash_key(book_name)
        if self.nodes[node_index].exists(book_name):
            print("The book ", book_name, "is stored in database node ", node_index)
        else:
            print("The book ", book_name, "is not found in the database.")

    ERROR_MESSAGE_INVALID_NODE = "The following Node doesn't exist."
    ERROR_MESSAGE_ALREADY_EMPTIED_NODE = "Node {} had already been emptied."
    INFO_MESSAGE_EMPTIED_NODE = "Node {} has been emptied."

    def empty_node(self, node_index):
        if 0 <= node_index <= 9:
            # Clear all entries in the specified node
            all_keys = list(self.nodes[node_index].getall())

            if len(all_keys) == 0:
                return self.ERROR_MESSAGE_ALREADY_EMPTIED_NODE.format(node_index)

            for key in all_keys:
                self.nodes[node_index].rem(key)

            # Save the changes
            self.nodes[node_index].dump()
            return self.INFO_MESSAGE_EMPTIED_NODE.format(node_index)

        else:
            return self.ERROR_MESSAGE_INVALID_NODE

    def empty_nodes(self, nodes_to_empty):
        messages = []
        for node_index in nodes_to_empty:
            messages.append(self.empty_node(node_index))
        return messages

    # TODO 1: implement this method as stated in the exercise description
    def doesDBContainKey(self, key: str):
        for index in self.nodes:
            if self.nodes[index].exists(key):
                return True
        else:
            return False
    
    # TODO 2: implement this method as stated in the exercise description
    def doesDBContainKeys(self, keys: list):
        for key in keys:
            temp = self.doesDBContainKey(key)
            if not temp:
                return False
            else:
                continue
        return True


    ERROR_MESSAGE_INVALID_DELTA = "The values still in the database are not what they should be"
    replicate_nodes = None

    # TODO 3: implement this method as stated in the exercise description
    def empty_nodes_check_remaining(self,nodes_to_empty=None):

        keysDeleted = []
        keysSaved = []
        nodes_to_empty = [int(n) for n in nodes_to_empty]

        for node in self.nodes:
            #for key in self.nodes[node].getall():
                if node in nodes_to_empty:
                    keysDeleted.extend(self.nodes[node].getall())
                else:
                    keysSaved.extend(self.nodes[node].getall())

        for node in self.nodes:
            if node in nodes_to_empty:
                self.empty_node(node)

        if keysDeleted:
           if not self.doesDBContainKeys(keysSaved):
               raise Exception(self.ERROR_MESSAGE_INVALID_DELTA)
           if self.doesDBContainKeys(keysDeleted):
               raise Exception(self.ERROR_MESSAGE_INVALID_DELTA)
           return  keysSaved, keysDeleted

        else:
            return keysSaved, keysDeleted


    
    # TODO 4: implement this method as stated in the exercise description
    def create_replicates(self):
        self.replicate_nodes = {}

        for node in self.nodes:
            replica = pickledb.load(f"replica.db", False)
            for key in  self.nodes[node].getall():
                replica.set(key,self.nodes[node].get(key))
            self.replicate_nodes [node] = replica

        return self.replicate_nodes



# TODO 5: implement this method as stated in the exercise description
    def recover_node(self, node_index):
        for key in self.replicate_nodes[node_index].getall():
            self.nodes[node_index].set(key,self.replicate_nodes[node_index].get(key))

        return self.nodes[node_index]
    
    # TODO 6: implement this method as stated in the exercise description
    def recover_nodes(self,nodes_to_recover):
        list = []

        for node_index in nodes_to_recover:
            self.recover_node(node_index)
            list.append(self.nodes[node_index])

        return list



with open('books.json', 'r') as json_file:
    books_data = json.load(json_file)

# Extract books list from JSON data
books = books_data['books']

sharded_db = ShardedDatabase()

sharded_db.create_replicates()

nodes_to_be_emptied = [3,4]
try:
    still_available, deleted = sharded_db.empty_nodes_check_remaining(nodes_to_be_emptied)
    print("Still available ", still_available)
    print("Deleted ", deleted)
except:
    print("empty_nodes_check_remaining()-method not implemented!")

sharded_db.recover_nodes(nodes_to_be_emptied)

for node_index in nodes_to_be_emptied:
    original_contents = list(sharded_db.nodes[node_index].getall())
    print(original_contents)
