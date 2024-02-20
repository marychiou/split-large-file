import os
import threading


# Define a function to write a chunk to a file
def write_chunk_to_file(chunk_lines, output_file_path):
    with open(output_file_path, 'wb') as output_file:
        output_file.writelines(chunk_lines)

def write_chunks_in_parallel():
    with open(input_file_path, 'rb') as input_file:
        # Calculate the number of lines in the input file
        num_lines = sum(1 for _ in input_file)
        print(num_lines)

        # Calculate the number of lines per thread
        lines_per_thread = num_lines // num_threads

        # Initialize the line counter
        line_counter = 0

        # Initialize the chunk counter
        chunk_counter = 1

        # Initialize a list to store the threads
        threads = []

        # Go back to the beginning of the file
        input_file.seek(0)
        input_file.readline()

        # Loop through the threads and create them
        for i in range(0, num_threads):
            # Calculate the output file path for this thread
            output_file_path = os.path.join(output_dir_path, f'chunk_{chunk_counter}.txt')

            # Initialize the list of lines for this thread
            thread_lines = []

            # Loop through the lines and add them to this thread's list
            while len(thread_lines) < lines_per_thread:
                line = input_file.readline()
                if not line:
                    break
                thread_lines.append(line)

            # Create a thread to write this chunk of lines to the output file
            thread = threading.Thread(target=write_chunk_to_file, args=(thread_lines, output_file_path))

            # Start the thread
            thread.start()

            # Add the thread to the list of threads
            threads.append(thread)

            # Increment the line counter
            line_counter += len(thread_lines)

            # Increment the chunk counter
            chunk_counter += 1

        # Wait for all the threads to finish
        for thread in threads:
            thread.join()

        # Check if there are any remaining lines
        remaining_lines = input_file.readlines()
        if remaining_lines:
            # Calculate the output file path
            output_file_path = os.path.join(output_dir_path, f'chunk_{chunk_counter}.txt')

            # Write the remaining lines to the output file
            with open(output_file_path, 'wb') as output_file:
                output_file.writelines(remaining_lines)

if __name__ == "__main__":
    # Define the input and output file paths
    input_file_path = 'C:/Users/user/Downloads/test.csv'
    output_dir_path = 'output'

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    # Define the number of threads
    num_threads = 50
    write_chunks_in_parallel()
