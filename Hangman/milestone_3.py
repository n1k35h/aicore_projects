import random # importing the random module from the python library

word_list = ['Apple', 'Banana', 'Strawberry', 'Orange', 'Pear'] # list of fruit
word = random.choice(word_list) # randomly chooses a fruit from the list

def check_guess(guess): # defining the check_guess and taking the guess parameter as an argument
    guess.lower()   # putting the users input as lower case
    if len(guess) == 1 and guess.isalpha(): # this is a if statement where users input is 1 character length long and its an alphabetical input
            if guess in word:   # another if statement where guess is in the word
                print(f"Good guess ,{guess}, is in the word.")  # if it is then print this statement
            else:                                               # else 
                print(f"Sorry, {guess} is not in the word")     # if it isn't then print this statement
    
def ask_for_input():    # defining the ask_for_input function, where user input their guess for the word
    check_guess(word)   # this calls the check_guess function and takes in the word variable
    guess = input("Please enter a single letter: ") # this takes the users input and once entered it will take the users input into a while loop
    while True: # if the while loop condition is True
        if len(guess) == 1 and guess.isalpha(): # this is a if statement where users input is 1 character length long and its an alphabetical input
            if guess in word:   # another if statement where guess is in the word
                print("Good guess!")  # if it is then print this statement
                break                 # break out of the loop
            else:                     # else
                print("Invalid letter. Please, enter a single alphabetical character.") # if it isn't then print this statement
                print(word) # and print the word
                break # break out of the loop

ask_for_input() # calls the ask_for_input function
