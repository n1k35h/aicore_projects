import random

word_list = ['Apple', 'Banana', 'Strawberry', 'Orange', 'Pear']
word = random.choice(word_list)

guess = input("Please enter a single letter: ")
if len(guess) == 1 and guess.isalpha():
    if guess in word:
        print("Good guess")
    else:
        print("Oops! That is not a valid input!")
    print(word)
