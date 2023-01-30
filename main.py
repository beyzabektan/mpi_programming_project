# Student Name: Beyzanur Bektan
# Student Number: 2019400174
# Compile Status: Compiling
# Program Status: Working
# Notes: Anything you want to say about your code that will be helpful in the grading process.

from mpi4py import MPI
import sys

world_comm = MPI.COMM_WORLD
n_rank = world_comm.Get_size()  # number of processes
rank = world_comm.Get_rank()  # index of the current rank


# This function takes a line and counts the unigrams in it and returns a dictionary that keeps the unigrams and the
# number of occurrences of them.
def unigram_word_counter(my_str, uni_count):
    unigrams = my_str.split()  # unigrams is the list that keeps the words in the line.
    for unigram in unigrams:
        uni_count[unigram] = uni_count.setdefault(unigram, 0) + 1  # For every word in the list, this line checks
        # whether the unigram is in dictionary or not. If unigram is in the dictionary it increments it 1, if not,
        # it adds unigram to dict and set value of it 1.


# This function takes a line and counts the bigrams in it and returns a dictionary that keeps the bigrams and the
# number of occurrences of them.
def bigram_word_counter(my_str, bi_count):
    words = my_str.split()  # unigrams is the list that keeps the words in the line.
    if len(words) == 0:  # if the line consists of the one word it means that there is no bigram so bi_count
        # dict is empty.
        return bi_count
    for i in range(len(words) - 1):
        bigram = words[i] + " " + words[i + 1]  # bigram variable keeps the bigrams
        bi_count[bigram] = bi_count.setdefault(bigram, 0) + 1  # For every bigram, this line checks
        # whether the bigram is in dictionary or not. If bigram is in the dictionary it increments it 1, if not,
        # it adds bigram to dict and set value of it 1


# This function takes an array that consists of many unigram dictionaries and bigram dictionaries, and merge them one
# by one. It returns merged unigram and bigram dictionary.
def merger(arr):
    uni_dict = dict()  # The unigram dictionary will keep all the unigrams and their number of occurrences.
    bi_dict = dict()  # The bigram dictionary will keep all the bigrams and their number of occurrences.
    for process in range(len(arr)):  # This for loop iterates for the dictionaries of every process.
        for key in arr[process][0]:
            uni_dict[key] = uni_dict.setdefault(key, 0) + arr[process][0][key]  # This line checks if specified
            # unigram is in the unigram dict of the iterated process. If it is in the unigram dict it adds their
            # value, if not, it sets the value of unigram to value of unigram in the dict of process.
        for key in arr[process][1]:
            bi_dict[key] = bi_dict.setdefault(key, 0) + arr[process][1][key]  # This line checks if specified
            # bigram is in the bigram dict of the iterated process. If it is in the bigram dict it adds their
            # value, if not, it sets the value of bigram to value of bigram in the dict of process
    return [uni_dict, bi_dict]


# This function takes two dictionaries and merge them. It returns merged dictionary.
def merger_two(dict1, dict2):
    for key in dict1:
        dict2[key] = dict2.setdefault(key, 0) + dict1[key]  # This line checks if key is in dict2 if key is in it, it
        # adds the value of it, if not, it sets the value.
    return dict2


# This is the master function. It takes input_line and test_line inputs. input_line input keeps the lines of input,
# and test_line input keeps the bigrams that will be tested. The master process allocates the data to workers,
# they make counting and returns the data back to master directly. Master process make merging and probability
# calculations.
def master(input_line, test_line):
    if rank == 0:  # if it is the master process
        data = [[] for _ in range(n_rank - 1)]  # This list keeps lists that will consists of lines for every
        # worker process.
        with open(input_line) as file:  # This line opens input_file
            counter = 0  # counter variable is used for allocation of data to workers equally
            for line in file:
                data[counter].append(line)  # This line appends lines to line list of workers in order.
                counter = (counter + 1) % (n_rank - 1)  # Updates counter variable as 1 to number of ranks.
            # to in front of the data for master process.
            for i in range(n_rank - 1):
                world_comm.send(data[i], dest=(i + 1))  # Sends data to workers.

    if rank != 0:  # If it is worker process.
        data = world_comm.recv(source=0)    # Receives data from master
        print('rank:', rank, 'number of sentences:', len(data))  # This line prints number of lines for every
        # worker process
        uni_count = dict()  # There is a uni_count dict for every worker process and keeps unigrams.
        bi_count = dict()  # There is a bi_count dict for every worker process and keeps bigrams.
        for line in data:  # For loop iterates for every line in the worker process.
            unigram_word_counter(line, uni_count)  # Calculates unigrams and their number of occurrences through
            # unigram_word_counter function.
            bigram_word_counter(line, bi_count)  # Calculates bigrams and their number of occurrences through
            # bigram_word_counter function.
        new_data = [uni_count, bi_count]  # data keeps calculated unigram and bigram dictionaries.
        world_comm.send(new_data, dest=0)   # Sends back calculated data to master

    if rank == 0:  # if it is the master process
        gathered_data = []
        for i in range(n_rank - 1):
            gathered_data.append(world_comm.recv(source=i+1))   # Receives calculated data from all workers
        last_arr = merger(gathered_data)  # last_arr variable keeps the merged dictionaries from all of the
        # workers' dictionaries
        with open(test_line) as file:  # This line opens test_line file.
            for line in file:
                splited_line = line.split()  # Splits line into words
                bigram = ' '.join(splited_line)  # creating bigram
                cond_prob = last_arr[1][bigram] / last_arr[0][splited_line[0]]  # For every test bigram, this line
                # calculates conditional probability that can be printed.
                print("The probability of " + bigram + " --> " + str(cond_prob))  # prints the conditional probability.


# This is the workers function. It takes input_line and test_line inputs. input_line input keeps the lines of input,
# and test_line input keeps the bigrams that will be tested. The master process allocates the data to workers,
# they make counting and returns the data to next worker process, then to master process. Master process make merging
# and probability calculations.
def workers(input_file, test_file):
    if rank == 0:  # if it is the master process
        new_data = [[] for _ in range(n_rank - 1)]  # This list keeps lists that will consists of lines for every
        # worker process.
        with open(input_file) as file:  # This line opens input_file
            counter = 0  # counter variable is used for allocation of data to workers equally
            for line in file:
                new_data[counter].append(line)  # This line appends lines to line list of workers in order.
                counter = (counter + 1) % (n_rank - 1)  # Updates counter variable as 1 to number of ranks.
            for i in range(n_rank - 1):
                world_comm.send(new_data[i], dest=(i + 1))  # Sends data to workers

    if rank == 1:  # If it is first worker process
        new_data = world_comm.recv(source=0)    # Receives data from master
        print('rank:', rank, 'number of sentences:', len(new_data))  # This line prints number of lines for every
        # worker process
        uni_count = dict()  # There is a uni_count dict for every worker process and keeps unigrams.
        bi_count = dict()  # There is a bi_count dict for every worker process and keeps bigrams.
        for line in new_data:  # For loop iterates for every line in the worker process.
            unigram_word_counter(line, uni_count)  # Calculates unigrams and their number of occurrences through
            # unigram_word_counter function.
            bigram_word_counter(line, bi_count)  # Calculates bigrams and their number of occurrences through
            # bigram_word_counter function.
        new_data = [uni_count, bi_count]  # new_data keeps calculated unigram and bigram dictionaries.
        world_comm.send(new_data, dest=(rank + 1))  # this line sends new_data to next worker process.
    elif rank > 1 and rank != (n_rank - 1):  # If rank of the process between 1 and the last process exclusively.
        new_data = world_comm.recv(source=0)
        print('rank:', rank, 'number of sentences:', len(new_data))  # This line prints number of lines for every
        # worker process
        new_data2 = world_comm.recv(source=(rank - 1))  # this line receives data from the previous worker process.
        uni_count = dict()  # There is a uni_count dict for every worker process and keeps unigrams.
        bi_count = dict()  # There is a bi_count dict for every worker process and keeps bigrams.
        for line in new_data:  # For loop iterates for every line in the worker process.
            unigram_word_counter(line, uni_count)  # Calculates unigrams and their number of occurrences through
            # unigram_word_counter function.
            bigram_word_counter(line, bi_count)  # Calculates bigrams and their number of occurrences through
        new_data = [merger_two(uni_count, new_data2[0]), merger_two(bi_count, new_data2[1])]  # This line merges
        # previous data and current data.
        world_comm.send(new_data, dest=rank + 1)  # this line sends new_data to next worker process.
    elif rank == (n_rank - 1):  # If rank is the rank of the last process
        new_data = world_comm.recv(source=0)
        print('rank:', rank, 'number of sentences:', len(new_data))  # This line prints number of lines for every
        # worker process
        new_data2 = world_comm.recv(source=(rank - 1))  # this line receives data from the previous worker process.
        uni_count = dict()  # There is a uni_count dict for every worker process and keeps unigrams.
        bi_count = dict()  # There is a bi_count dict for every worker process and keeps bigrams.
        for line in new_data:  # For loop iterates for every line in the worker process.
            unigram_word_counter(line, uni_count)  # Calculates unigrams and their number of occurrences through
            # unigram_word_counter function.
            bigram_word_counter(line, bi_count)  # Calculates bigrams and their number of occurrences through
        new_data = [merger_two(uni_count, new_data2[0]), merger_two(bi_count, new_data2[1])]  # This line merges
        # previous data and current data.
        world_comm.send(new_data, dest=0)  # this line sends new_data to master process.

    if rank == 0:  # If it is first worker process
        last_arr = world_comm.recv(source=(n_rank - 1))  # Master process receives the merged data.
        with open(test_file) as file:  # This line opens test_file file.
            for line in file:
                splited_line = line.split()  # Splits line into words
                bigram = ' '.join(splited_line)  # creating bigram
                cond_prob = last_arr[1][bigram] / last_arr[0][splited_line[0]]  # For every test bigram, this line
                # calculates conditional probability that can be printed.
                print("The probability of " + bigram + " --> " + str(cond_prob))  # prints the conditional probability.


input_file = ""  # input_file is the file that contains lines.
test_file = ""  # test_file is the file that contains test bigrams.
merge_method = ""  # merge_method is the merging method master() or workers()

for arg_index in range(len(sys.argv)):
    if sys.argv[arg_index] == "--input_file":  # If there is --input_file in arguments, the next argument will
        # be input_file
        input_file = sys.argv[arg_index + 1]
    if sys.argv[arg_index] == "--merge_method":  # If there is --merge_method in arguments, the next argument will
        # be merge_method
        merge_method = sys.argv[arg_index + 1]
    if sys.argv[arg_index] == "--test_file":  # If there is --test_file in arguments, the next argument will be
        # test_file
        test_file = sys.argv[arg_index + 1]

if merge_method == "MASTER":  # If merge_method is MASTER call master function
    master(input_file, test_file)
elif merge_method == "WORKERS":  # If merge_method is WORKERS call workers function
    workers(input_file, test_file)
