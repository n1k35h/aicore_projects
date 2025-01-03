# Computer Vision RPS
# Milestone 1
Setting up GitHub and created a GitHub repository to track the codeof the Computer Vision Rock Paper Scissors project.

# Milestone 2
Created an image project model with 4 different classes that would be a part of a game - Rock, Paper, Scissors and Nothing. The "Nothing" class will present the lack of movement. The web-based tool that was used to create and capture the hand movement is called Teachable-Machine.

Rock:
![rock](https://user-images.githubusercontent.com/119499198/218551051-c689017d-bf2c-4335-87b8-e7d5bc291c0d.jpg)

Paper:
![paper](https://user-images.githubusercontent.com/119499198/218551081-b026809e-3945-4589-873b-57fafd404994.jpg)

Scissors:
![scissors](https://user-images.githubusercontent.com/119499198/218551099-a4ecd81e-6f28-4941-97d5-e560157e70c8.jpg)

After creating the 4 different classes for the image project model, I download the model from the "Tensorflow" tab in the Teachable-Machine tool. The 2 files that are downloaded were keras_model.h5, which is a model structure and labels.txt, which is the parameters of a deep learning model.

# Milestone 3
Before entering any code for this project or using the model, a conda environment need to be created by entering conda create -n rps, which the rps is the environment that is created in conda for this project. conda activate rps to activate the environment and then install pip by entering conda install pip, which the pip will be used for installing various libraries. 3 Libraries that needs to be installed for this project opencv-python, tensorflow and ipykernel.

# Milestone 4
Created a file called manual_rps.py, which will be used to play the game manually without the use of the camera. The import random module is required for getting the Rock, Paper and Scissors option randomly. There are 2 functions that needs to be created for this game, which are get_computer_choice and get_user_choice. get_computer_choice function will have the random.choice as the computer input will be a randomly chosen from the list. Whereas the get_user_choice function will have user to input their choice manually. 

So the code for the get_computer_choice is as follows:

def get_computer_choice():
    computer_choice = random.choice(["Rock", "Paper", "Scissors"])
    print(f"The Computer's choice is: {computer_choice}")
    return computer_choice
    
and the code for get_user_choice is as follows:

def get_user_choice():
    user_input = input("What is your choice? ")
    print(f"\nUser input is: {user_input}")
    return user_input
    
get_winner() function is created and passing 2 arguments in the parentheses, which are computer_choice and user_choice

def get_winner(computer_choice=str, user_choice=str):

In this get_winner function, it will have if-elif-else statements, which will define if whether the computer won or user won or if it's a tie in the game.

play() function is to run the game of the code

Here is a part from the game:

<img width="567" alt="2023-01-12" src="https://user-images.githubusercontent.com/119499198/212205014-526be933-7b8f-44d7-9c6c-39338772dda3.png">


# Milestone 5
camera_rps.py file is created to produce Rock, Paper, Scissors and Nothing game by using the Camera function. get_prediction() is created as a function to get the user choice as determined by Keras CV Model that would than passed to get_winner function, which would then decide who won that game.

Countdown was added to allow user to be prepared before the camera captures the image of the user's hand.

Best of 5 games and first player to reach 3 wins is added to the play() function.

play_again is added in play() function as an extra feature of the game
