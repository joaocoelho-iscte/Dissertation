def main():
    # Initialize a list of 80 zeros to store partitions
    IP = [0] * 80
    
    # Loop through numbers 1 to 80
    for N in range(1, 81):
        # Print the current number N with width 10
        print(f"{N:10d}")
        
        # Initialize partition: first element is N, rest are zeros
        # This represents the trivial partition [N] + 79 zeros
        IP = [N] + [0] * 79
        # Print the initial partition (all numbers formatted to width 3)
        print("".join(f"{val:3d}" for val in IP))
        
        # Main loop to generate all partitions for current N
        while True:
            # Find the rightmost part that is greater than 1
            # J will be the index of first part <= 1 or end of array
            J = 0
            while J < 80 and IP[J] > 1:
                J += 1
            
            # J now points to first part <= 1 or end of array
            
            # If J == 0, all parts are 1 (or array empty), we're done
            if J == 0:  # All parts are 1, we're done
                break
            
            # The part to modify is at position J-1 
            # (since we overshot by 1 in the while loop)
            part_to_modify = J - 1
            
            # Calculate sum of all parts before the one we're modifying
            sum_before = sum(IP[:part_to_modify])
            # Calculate remaining value to distribute
            remaining = N - sum_before
            
            # Get current value of the part we're modifying
            current_value = IP[part_to_modify]
            # Decrease this part by 1 for the next partition
            new_value = current_value - 1
            
            # Calculate how many times we can use the new value
            count = remaining // new_value
            # Calculate remainder after using new_value 'count' times
            remainder = remaining % new_value
            
            # Update the partition: replace with 'count' copies of new_value
            for i in range(count):
                if part_to_modify + i < 80:
                    IP[part_to_modify + i] = new_value
            
            # Add the remainder as the next part (if any)
            if part_to_modify + count < 80:
                IP[part_to_modify + count] = remainder
            
            # Zero out all remaining positions after what we just filled
            next_position = part_to_modify + count + (1 if remainder > 0 else 0)
            for i in range(next_position, 80):
                IP[i] = 0
            
            # Print the new partition
            print("".join(f"{val:3d}" for val in IP))
        
        # Empty line between different N values
        print()
    
    print("Program completed")

if __name__ == "__main__":
    main()