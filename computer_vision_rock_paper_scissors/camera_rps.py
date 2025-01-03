import cv2
from keras.models import load_model
import numpy as np
import random
import time

# countdown function to start the game
def countdown():
        timer = 5   # 5 seconds
        print("Show your hand in...")   # mesaage show before the countdown starts
        while int(timer) > 0:
            cv2.waitKey(1000)       #cv2 module required
            print("{}".format(timer))
            time.time()
            timer -= 1

def get_prediction():
    model = load_model('keras_model.h5')
    cap = cv2.VideoCapture(0)   # Open the camera
    rps_list = ['Rock', 'Paper', 'Scissors', 'Nothing'] # list of items for the game
    data = np.ndarray(shape=(1, 224, 224, 3), dtype=np.float32)

    # begin of the video prediction while loop
    while True: 
        ret, frame = cap.read()
        resized_frame = cv2.resize(frame, (224, 224), interpolation = cv2.INTER_AREA)
        image_np = np.array(resized_frame)
        normalized_image = (image_np.astype(np.float32) / 127.0) - 1 # Normalize the image
        data[0] = normalized_image
        cv2.imshow('RPS', frame)    # displays the video frame for the user to view what hand gesture the camera has captured
        prediction = model.predict(data) # model to predict what the current image it is 
        user_choice = rps_list[np.argmax(prediction[0])]  # displays the predicted text of the user's choice
        cv2.waitKey(5000)   # camera show for 5 seconds
        print(user_choice)
        break   # breaks the while loop

        # Press q to close the window
        # if cv2.waitKey(1) & 0xFF == ord('q'):
        #     break
        
    # After the loop release the cap object
    cap.release()
    # Destroy all the windows
    cv2.destroyAllWindows()

    return user_choice

def get_user_choice():
    return get_prediction() # returns the get_prediction function

def get_computer_choice():
    computer_choice = random.choice(["Rock", "Paper", "Scissors"])  # a list where a computer will choice at random option
    # print(f"The Computer's choice is: {computer_choice}")
    return computer_choice  # returns a random chose for the computer

# method of winning a game
def get_winner(computer_choice, user_choice):
    # print(f"User choice is: {user_choice}")
    if computer_choice == "Rock" and user_choice == "Rock": # both selected the sasme option
        # print("Computer chooses Rock")
        print("It is a tie")
        result = "no winner"

    elif computer_choice == "Rock" and user_choice == "Scissors":   # Rock beats Scissors
        # print("Computer choose Rock")
        print("You lost")
        result = "computer"

    elif computer_choice == "Rock" and user_choice == "Paper": # Paper beats Rock
        # print("Computer choose Rock")
        print("You won")
        result = "user"
    
    elif computer_choice == "Paper" and user_choice == "Paper": # Both selected the same option
        # print("Computer chooses Paper")
        print("It is a tie")
        result = "no winner"

    elif computer_choice == "Paper" and user_choice == "Scissors":  # Scissors beats Paper
        # print("Computer choose Paper")
        print("You won")
        result = "user"

    elif computer_choice == "Paper" and user_choice == "Rock":  # Paper beats Rock
        # print("Computer choose Paper")
        print("You lost")
        result = "computer"

    elif computer_choice == "Scissors" and user_choice == "Scissors":   # Both selecte the same option
        # print("Computer chooses Scissors")
        print("It is a tie")
        result = "no winner"

    elif computer_choice == "Scissors" and user_choice == "Rock":   # Rock beat Scissors
        # print("Computer choose Scissors")
        print("You won")
        result = "user"

    elif computer_choice == "Scissors" and user_choice == "Paper":    # Scissors beats Paper
        # print("Computer choose Scissors")
        print("You lost")
        result = "computer"
    else:
        user_choice == "Nothing"
        print("You didn't play this round as there was no valid chose!")
        result  = "no winner"

    return result

def play():
    round_played = 0
    computer_wins = 0
    user_wins = 0
    
    while round_played <= 5: # number of games played 
        start_time = countdown()    # starting the countdown
        computer_choice = get_computer_choice()
        user_choice = get_user_choice()

        # display what each player has choosen 
        print("")
        print(f"Computer choice is: {computer_choice}")
        print(f"User choice is: {user_choice}")
        print("")

        # display the result of each game e.g: you won, you lost or no winner
        result = get_winner(computer_choice, user_choice)

        # gives a point to the winner otherwise if it's a tie no point 
        if result == "computer":
            computer_wins += 1
        elif result == "user":
            user_wins += 1
        else: 
            result == "no winner"
        round_played += 1

        # displays the number of rounds played and scores for each players
        print("")
        print(f"Round Played: {round_played}")
        print(f"Computer wins: {computer_wins}, User wins: {user_wins}")
        print("")

        # if a player wins 3 games then it ends the game
        if computer_wins == 3:
            print("Computer wins")
            break
        elif user_wins == 3:
            print("User wins")
            break
    
    while True:
        play_again = input("Play again: Yes(y) or No(n): ")
        if play_again.lower() == "y":
            play()
        else:
            play_again.lower() == "n"
            print("Game Over!")
            break

play()
