# Hangman
Hangman is a classic game in which a player thinks of a word and the other player tries to guess that word within a certain amount of attempts.

This is an implementation of the Hangman game, where the computer thinks of a word and the user tries to guess it. 

In the milestone_2.py file, on the 1st line I have imported a module called random, which will get an element randomly from a list

On line 3, I have created a variable called word_list and in this list it contains various fruit
word_list = ['Apple', 'Banana', 'Strawberry', 'Orange', 'Pear']

On line 4, this is where the import random module come in play, I have assigned a variable called word and in this variable I have a random.choice function, which is a function sequence that will get a word at random from the list created in word_list variable.

On line 6, variable guess is created for user input, where it will ask for a user to enter a letter of the randomly chosen word  

On line 7 to 12, it has couple of if statement, first if statement instructs the length of user guessed character to equal to 1 character and the character should be an alphabet - guess.isalpha(). Second if statement from 8 to 11 instruct that if the guess letter is in the word then print out "Good guess" else "Oops! That is not a valid input!". Line 12 prints out the word of the game.

In the milestone_3.py file, it contains 2 defined function, which are check_guess and ask_for_input. 
In the check_guess function guess is passed as a parameter as an argument. Line 7 is to make sure that the guess letter that user inputted is in lowercase - guess.lower(). On line 8 to 12, it has couple of if statement, first if statement instructs the length of user guessed character to equal to 1 character and the character should be an alphabet - guess.isalpha(). Second if statement from 9 to 12 instruct that if the guess letter is in the word then print out f"Good guess ,{guess}, is in the word." else f"Sorry, {guess} is not the word". {guess} is the letter that the user inputted. 

In the ask_for_input funtion, is where user input the letter of the randomly chosen word. Line 15, calls the check_guess(word) function and takes the word variable as an argument. Line 16, is where user will input their chosen letter of the randomly chosen word. Line 17 to 25 contains a while loop, where on line 17 the condition is True: if the criteria is met. 

On line 27, the ask_for_input is called to run the code.

In the milestone_4.py file, there is a class function called Hangman, which is an object that contains 3 defined methods - __init__, check_guess and ask_for_input. In the first defined method called __init__, contains the initialise of the attribute in the class where self, word_list and num_lives are passed through as parameters, which lays in-between the parentheses of the __init__ method. Inside the __init__ method contains a list of 6 various self.variables which act as the attribute of the class. The 6 self.variablesSuch as:
• self.word_list = word_list # is a variable that contains a list of fruit, which also passed as parameter that is in-between the parentheses for __init__ method
• self.word = random.choice(word_list) # is a variable for word that randomly choose a word from the word_list
• self.word_guessed = ['_'] * len(self.word) # is a variable for a list of letters of the word with _ (underscore) for each letter not guessed yet and if the correct letter is guessed then the _ (underscore) is replaced with the correct letter
• self.num_letters = len(set(self.word)) # is a variable for a number of unique letters in the word that has not been guessed
• self.num_lives = num_lives # is a variable for lives that a player has in a game
• self.list_of_guessed = [] # is a variable for a list of letters that has already been guessed. [] - square brackets is created as an empty list

In the second defined method called check_guess, where it will check the player's guessed letter is in the word or not. The guess variable is passed as an argument, which lay in-between the parentheses of the check_guess method. In line 13, guess.lower() converts the player's guessed letter to lowercase. In line 14, there is an if statement that tells the computer to check that the guessed letter is equal to 1 character length and if it's an alphabet character. If the guessed letter passes the first if statement it will then go to the second if statement, which tells the computer to check that the guessed letter is in the word and if it is then the print statement is printed "Good guess! a is in the word.". If the guessed letter passes the second if statement then it will go to the for loop, which tells the computer to add the letter to the word and reduce the number of letters by 1 self.num_letter -= 1. If the guessed letter is not in the word then the else statement is called and in line 24, self.num_lives -= 1 it will tell the computer to take a live from the player and will print couple of statement "Sorry, a is not in the word!" and "You have 4 lives left.".

In the third defined method called ask_for_input, where it will ask the player to input a guessed letter for the randomly chosen word. A while loop is created where condition is set to True. A guess variable is created for the computer to ask the player to input a guessed letter. If the letter is incorrect and it's not a valid letter then the print statement is printed "Invalid letter. Please, enter a single alphabetical character.". Otherwise, if the letter is valid and the letter has already been guessed then the else-if statement is called printing "You already tried that letter!". However, if the player's guessed letter is valid and the letter is in the word or not than it goes to the else statement where the check_guess method is called and guess is passed as an argument. In line 37, the valid letter whether it's in the word or not will be added to the self.list_of_guesses list. In line 38 the break function is called to come out of the while loop. 

In line 40, the game variable is created to pass the class Hangman object where in the parentheses it has couple of parameters called word_list and num_lives.
In line 41, the game variable is calling for ask_for_input method to run the code - game.ask_for_input()

In milestone_5.py file, play_game function is created with word_file passed as a parameter. On line 41 an instance of the Hangman class is assign to a variable called game. Where the word_list and num_lives=5 is pass as an argument to the game object. On line 42 a while loop is created where condition is set to True. Whilst in the while loop an if statement is created where if the num_lives is equal to 0 then the print statement is printed "You Lost" and this will break the while loop. On line 46, it will check if the num_letter is greater than 0 then the game will continue by calling the ask_for_input() method. This will continue until the player lose all 5 lives or guess the word correctly. The final if statement will be called if the num_lives is not 0 and the num_letters is not greater than 0 then the player has won the game and the print statement is printed "Congratulation. You won the game." Once the player has won the game it should break the while loop.

In line 55, the word_list is created as a variable with list of fruit passed as a string in-between the square brackets
In line 56, the play_game is called with the word_list passed as an argument in the parentheses
