#--------------------------------------------------------------------------------------------------------------
#   Dependancies
#--------------------------------------------------------------------------------------------------------------

import sys
import time

#--------------------------------------------------------------------------------------------------------------
#   Functions
#--------------------------------------------------------------------------------------------------------------

def wait(duration=0.1):
    "Waits a specified time."
    time.sleep(duration)

def clear():
    "Clears the console."
    print("\033[H\033[J", flush=True)

def hide_cursor():
    "Hides the console cursor."
    sys.stdout.write("\033[?25l")

def show_cursor():
    "Unhides the console cursor."
    sys.stdout.write("\033[?25h")

colors = {
    "green": "\033[32m",
    "light_green": "\033[92m",
    "red": "\033[31m",
    "white": "\033[37m",
    "yellow": "\033[33m",
    "bright_yellow": "\033[93m",
    "cyan": "\033[36m",
    "blue": "\033[34m",
    "light_blue": "\033[94m",
    "light_grey": "\033[37m",
    "grey": "\033[90m",
    "purple":"\033[35m"
}

green, light_green, red, white, yellow, bright_yellow, cyan, blue, light_blue, light_grey, grey, purple = colors.values()

cursor_control = {
    "up": "\033[A",
    "down": "\033[B",
    "right": "\033[C",
    "left": "\033[D",
    "next_line": "\n",
    "prev_line": "\033[F",
    "clear_line": " \x1b[2K\r"
}
up, down, right, left, down, up, clear_line = cursor_control.values()

lines_wanted = 50

def return_loading_string(progress:int, length:int, seperate_string=False) -> tuple[str, str]|tuple[str, str, str, str]:
    "Returns a loading string. If seperate_string is True, it'll return the dots, percent, and loading bar seperately."
    dots = '.'*(progress % 4)
    if length == 0:
        return '0%', f'{white}\r'
    else:
        percent = int((progress/length)*100) if progress != 0 else 0
    loading_bar = f'{light_green}█' * ( int( (progress / length ) * lines_wanted ) ) + f"{green}░" * (lines_wanted - int( (progress / length ) * lines_wanted ))
    if not seperate_string:
        return f"{dots.ljust(4)} {str(percent).ljust(2)}% {loading_bar} ", f'{white}\r'
    else:
        return f"{dots.ljust(4)}", f"{str(percent).ljust(2)}%", f"{loading_bar} ", f'{white}\r'
