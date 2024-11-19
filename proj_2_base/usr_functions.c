#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include "common.h"
#include "usr_functions.h"

#define BUFFER_SIZE 4096

/* User-defined map function for the "Letter counter" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int letter_counter_map(DATA_SPLIT * split, int fd_out)
{
    if (!split || split->fd < 0 || split->size <= 0 || fd_out < 0) {
        return -1;
    }
    char *buffer = (char *)malloc(split->size);
    if (!buffer) {
        perror("Memory allocation failed");
        return -1;
    }

    ssize_t bytes_read = read(split->fd, buffer, split->size);
    if (bytes_read < 0) {
        perror("Error reading from file descriptor");
        free(buffer);
        return -1;
    }

    int letter_counts[26] = {0};

    for (ssize_t i = 0; i < bytes_read; i++) {
        if (isalpha(buffer[i])) {
            char letter = toupper(buffer[i]);
            letter_counts[letter - 'A']++;
        }
    }

    for (int i = 0; i < 26; i++) {
        char output_line[32];
        int len = snprintf(output_line, sizeof(output_line), "%c %d\n", 'A' + i, letter_counts[i]);
        if (write(fd_out, output_line, len) != len) {
            perror("Error writing to intermediate file");
            free(buffer);
            return -1;
        }
    }

    free(buffer);
    return 0;
    
}

/* User-defined reduce function for the "Letter counter" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out) {

    if (p_fd_in == NULL || fd_in_num <= 0 || fd_out < 0) {
        return -1; // Error handling
    }

    int letter_counts[26] = {0}; // For letters A-Z

    // Read from each intermediate file
    for (int i = 0; i < fd_in_num; i++) {
        FILE *file = fdopen(p_fd_in[i], "r");
        if (file == NULL) {
            return -1; // Error handling
        }

        char line[128]; // Buffer for reading each line
        while (fgets(line, sizeof(line), file)) {
            char letter;
            int count;

            // Parse line in the format "X 123"
            if (sscanf(line, "%c %d", &letter, &count) == 2) {
                if (letter >= 'A' && letter <= 'Z') {
                    letter_counts[letter - 'A'] += count; // Aggregate counts
                }
            }
        }

        fclose(file); // Close the current intermediate file
    }

    // Write the aggregated results to the final output file
    for (int i = 0; i < 26; i++) {
        if (letter_counts[i] > 0) {
            char output[32];
            int len = snprintf(output, sizeof(output), "%c %d\n", 'A' + i, letter_counts[i]);
            write(fd_out, output, len); // Write to the output file
        }
    }

    return 0; 
}

/* User-defined map function for the "Word finder" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int word_finder_map(DATA_SPLIT * split, int fd_out)
{   
    if (!split || !split->usr_data || split->size <= 0) {
        fprintf(stderr, "Invalid input to word_finder_map\n");
        return -1;
    }

    const char *word = (const char *)split->usr_data;  // The word to find
    size_t word_len = strlen(word);  // Length of the word
    if (word_len == 0) {
        fprintf(stderr, "Empty word to search for\n");
        return -1;
    }

    char *buffer = malloc(split->size + word_len);  // Allocate a buffer for the split size + extra for boundary handling
    if (!buffer) {
        perror("Failed to allocate memory");
        return -1;
    }

    // Read the data split from the file descriptor
    ssize_t bytes_read = read(split->fd, buffer, split->size);
    if (bytes_read < 0) {
        perror("Failed to read from input file descriptor");
        free(buffer);
        return -1;
    }

    // Handle boundary overlap by reading the extra bytes (if available)
    ssize_t extra_bytes = read(split->fd, buffer + bytes_read, word_len - 1);
    if (extra_bytes > 0) {
        bytes_read += extra_bytes;
    }

    // Null-terminate the buffer for safety
    buffer[bytes_read] = '\0';

    // Search for the word in the buffer
    size_t position = 0;  // Position within the current split
    int matches_found = 0;

    for (char *ptr = buffer; (ptr = strstr(ptr, word)) != NULL; ptr++) {
        position = ptr - buffer;

        // Write the position to the output file
        char output[64];
        int output_len = snprintf(output, sizeof(output), "Position: %zu\n", position);
        if (write(fd_out, output, output_len) < 0) {
            perror("Failed to write to intermediate file");
            free(buffer);
            return -1;
        }

        matches_found++;
    }

    free(buffer);
    return 0;
}

/* User-defined reduce function for the "Word finder" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    if (!p_fd_in || fd_in_num <= 0 || fd_out < 0) {
        fprintf(stderr, "Invalid input to word_finder_reduce\n");
        return -1;
    }

    // Buffer to hold positions from all intermediate files
    size_t positions_size = 1024;
    size_t positions_count = 0;
    size_t *positions = malloc(positions_size * sizeof(size_t));
    if (!positions) {
        perror("Failed to allocate memory for positions");
        return -1;
    }

    // Read and aggregate positions from intermediate files
    for (int i = 0; i < fd_in_num; i++) {
        char buffer[256];
        ssize_t bytes_read;

        while ((bytes_read = read(p_fd_in[i], buffer, sizeof(buffer) - 1)) > 0) {
            buffer[bytes_read] = '\0'; // Null-terminate the buffer

            // Parse the buffer line by line
            char *line = strtok(buffer, "\n");
            while (line != NULL) {
                size_t position;
                if (sscanf(line, "Position: %zu", &position) == 1) {
                    // Store the position
                    if (positions_count >= positions_size) {
                        positions_size *= 2;
                        size_t *new_positions = realloc(positions, positions_size * sizeof(size_t));
                        if (!new_positions) {
                            free(positions);
                            perror("Failed to reallocate memory for positions");
                            return -1;
                        }
                        positions = new_positions;
                    }
                    positions[positions_count++] = position;
                }
                line = strtok(NULL, "\n");
            }
        }

        if (bytes_read < 0) {
            perror("Failed to read from intermediate file");
            free(positions);
            return -1;
        }
    }

    // Sort the positions (if needed)
    qsort(positions, positions_count, sizeof(size_t), (int (*)(const void *, const void *))strcmp);

    // Write the final result to the output file
    for (size_t i = 0; i < positions_count; i++) {
        char output[64];
        int output_len = snprintf(output, sizeof(output), "Position: %zu\n", positions[i]);
        if (write(fd_out, output, output_len) < 0) {
            perror("Failed to write to result file");
            free(positions);
            return -1;
        }
    }

    free(positions);
    return 0;

}


