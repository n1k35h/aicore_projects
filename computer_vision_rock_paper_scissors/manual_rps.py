import random

def get_computer_choice():
    computer_choice = random.choice(["Rock", "Paper", "Scissors"])
    print(f"The Computer's choice is: {computer_choice}")
    return computer_choice

def get_user_choice():
    user_input = input("What is your choice? ")
    print(f"User input is: {user_input}")
    return user_input

def get_winner(computer_choice=str, user_choice=str):
    if computer_choice == "Rock" and user_choice == "Rock":
        # print("Computer chooses Rock")
        print("It is a tie!")

    elif computer_choice == "Rock" and user_choice == "Scissors":
        # print("Computer choose Rock")
        print("You lost")

    elif computer_choice == "Rock" and user_choice == "Paper":
        # print("Computer choose Rock")
        print("You won!")
    
    elif computer_choice == "Paper" and user_choice == "Paper":
        # print("Computer chooses Paper")
        print("It is a tie!")

    elif computer_choice == "Paper" and user_choice == "Scissors":
        # print("Computer choose Paper")
        print("You won!")

    elif computer_choice == "Paper" and user_choice == "Rock":
        # print("Computer choose Paper")
        print("You lost")

    elif computer_choice == "Scissors" and user_choice == "Scissors":
        # print("Computer chooses Scissors")
        print("It is a tie!")

    elif computer_choice == "Scissors" and user_choice == "Rock":
        # print("Computer choose Scissors")
        print("You won!")

    elif computer_choice == "Scissors" and user_choice == "Paper":
        # print("Computer choose Scissors")
        print("You lost")
    else:
        ("Please enter a valid choice!")

def play():
    user_choice = get_user_choice()     # Calls the get_user_choice function
    computer_choice = get_computer_choice()     # calls the get_computer_choice function

    return get_winner(computer_choice=computer_choice, user_choice=user_choice)     # calls the get_winner function

play()
