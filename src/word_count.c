#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <pthread.h>
#include <linux/errno.h>

#define MAX_CHARS 26
#define WORD_LEN  75

/* split file 1 */
char *file_addr  = NULL;
char *file_start = NULL;
char *file_end   = NULL;

/* split file 2 */
char *file_addr1  = NULL;
char *file_start1 = NULL;
char *file_end1   = NULL;

/* split file 3 */
char *file_addr2  = NULL;
char *file_start2 = NULL;
char *file_end2   = NULL;


int io_thread_end = 0;
int no_words = 0;

unsigned total_unique_words = 0;
unsigned total_valid_words = 0;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

struct word_node {
	char word[WORD_LEN];
	unsigned long frequency;
	struct word_node *next;	
};

struct word_node *heap_node = NULL;

struct hash {
	struct word_node *head;
};

struct hash *hash_table = NULL;

pthread_t t1_io, t2_io, t3_io;
pthread_t t1_cpu;

void insert_to_hash(char *word_entry) {
	int key;
	struct word_node *node = NULL;

	node = (struct word_node*)malloc(sizeof(struct word_node));
	if(!node) {
		printf("Error: Unable to create word node\n");
		return;
	}
	
	strcpy(node->word, word_entry);
	node->frequency = 1;

	key = word_entry[0] - 'A';

	if(!hash_table[key].head) {
		hash_table[key].head = node;	
		return;
	}
	
	pthread_mutex_lock(&mutex);

	node->next = hash_table[key].head;
	hash_table[key].head = node;
	total_unique_words++;

	pthread_mutex_unlock(&mutex);
	
	return;	
	
}

int check_word_is_existing(char *new_word) {
	int key = 0;
	int found  = 0;
	struct word_node *node = NULL;


	key = new_word[0] - 'A';

	if(!hash_table[key].head) {
		return found;
	}

	
	node = hash_table[key].head;

	while(node != NULL) {
		if(!strcmp(node->word, new_word)) {
			pthread_mutex_lock(&mutex);
			node->frequency++;
			pthread_mutex_unlock(&mutex);
			found = 1;
			break;
		}
		node = node->next;
	}

	return found;	
}

void is_good_word(char *start, char *end) {

	int i = 0;
	char word[WORD_LEN];

	if(strncmp(start, "http", strlen("http") == 0))
		return;
	else {
		while (start <= end) {
			if((((*start >= 'A') && (*start <= 'Z')) ||((*start >= 'a') \
									 && (*start <= 'z')))) 
				word[i++] = toupper(*start);
			else {
				if((i > 0) && (*start == '-')) {
					word[i++] = *start;
					start++;
					continue;
				}
				break;
			}
			start++;
		}

		word[i] = '\0';
	}


	if((i > 0) && (*start != '.')) {
		total_valid_words++;
		if(!check_word_is_existing(word)) 
			insert_to_hash(word);
					
	}

	return;
}

void *parse_file1(void *arg) {

	char *current = NULL;
	char *word_start = NULL;
	int inside_word = 0;

	current = file_start;

	while(current <= file_end) {

 		if(inside_word == 1) {
       			if(*current == ' ' || current == file_end) {
				is_good_word(word_start, current-1);
      				inside_word = 0;
				word_start = NULL;
			}
		} else {
				if(*current != ' ') {
					word_start = current;
					inside_word = 1;
 
				}
		}  
		current++;
	}
		
	printf("\nPthread 1 Finished Processing Split File 1\n");

	pthread_exit((void *)1);
}

void *parse_file2(void *arg) {

	char *current = NULL;
	char *word_start = NULL;
	int inside_word = 0;

	current = file_start1;

	while(current <= file_end1) {

 		if(inside_word == 1) {
       			if(*current == ' ' || current == file_end1) {
				is_good_word(word_start, current-1);
      				inside_word = 0;
				word_start = NULL;
			}
		} else {
				if(*current != ' ') {
					word_start = current;
					inside_word = 1;
 
				}
		}  
		current++;
	}

	printf("\nPthread 2 Finished Processing Split File 2\n");
	pthread_exit((void *)1);
}

void *parse_file3(void *arg) {

	char *current = NULL;
	char *word_start = NULL;
	int inside_word = 0;

	current = file_start2;

	while(current <= file_end2) {

 		if(inside_word == 1) {
       			if(*current == ' ' || current == file_end2) {
				is_good_word(word_start, current-1);
      				inside_word = 0;
				word_start = NULL;
			}
		} else {
				if(*current != ' ') {
					word_start = current;
					inside_word = 1;
 
				}
		}  
		current++;
	}

	printf("\nPthread 3 Finished Processing Split File 3\n");
	pthread_exit((void *)1);
}

void print_top_entries() {
	int i = 0;
	struct word_node *node = heap_node;	

	for(i = 0; i < no_words; i++) {
		printf("\n===================\n");
		printf("Word : %s\n", node->word);
		printf("Occurences : %ld\n\n", node->frequency);
		node++;
	}

	printf("\n===================\n");

}

void free_hash_entries() {
	int i = 0;
	struct hash *loop = hash_table;
	struct word_node *node, *temp_node;

	for(i = 0; i < MAX_CHARS; i++) {
		if(loop[i].head != NULL) {
			node = loop[i].head;
			while(node != NULL) {
				temp_node = node->next;
				free(node);
				node = temp_node; 
			}			
		}
	}

}

void swap(struct word_node *node1, struct word_node *node2) {
	struct word_node temp_node; 

	memcpy(&temp_node, node2, sizeof(struct word_node));
	memcpy(node2, node1, sizeof(struct word_node));
	memcpy(node1, &temp_node, sizeof(struct word_node));
}

void sort_descending(struct word_node *word) {
	int i,j;
    
	for (i = 0; i < no_words; i++) {
		for (j = i + 1; j < no_words; j++){
            		if (word[i].frequency < word[j].frequency) {
				swap((word + i), (word + j));
            		}
        	}
    	}

}

void insert_word_to_heap(struct word_node *words, struct word_node *new) {
	int i;
	struct word_node nodes[no_words];

	memcpy(&nodes, words, no_words * sizeof(struct word_node));

	for(i = 0; i < no_words; i++) {
		if(words[i].frequency < new->frequency) {
			memcpy((words + i), new, sizeof(struct word_node));
			memcpy((words + ( i + 1)), (nodes + i), ((no_words - 1) - i) * \
									sizeof(struct word_node));
			break;
		}
	}

}

void insert_word(struct word_node *word_list, struct word_node *new) {
	int i = no_words - 1;
	
	int min = word_list[i].frequency;
	int max = word_list[0].frequency;

	if(new->frequency >= max || (new->frequency < max && new->frequency >= min)) {
		insert_word_to_heap(word_list, new);		
	}
	
	return;
}

void *find_top_occurences(void *arg) {
	int i = 0, j = 0;
	struct word_node *node, *heap_ptr = arg;
	struct word_node *temp_ptr = heap_ptr;
	
	for(i = 0; i < MAX_CHARS; i++) {
    		if(hash_table[i].head != NULL) {
			node = hash_table[i].head;
			while(node){
				if(j < no_words) {
					memcpy(temp_ptr,node, sizeof(struct word_node));			
					temp_ptr++;
					j++;
					if(j == no_words) {		
						sort_descending(heap_ptr);
					}
	
				} else {
					insert_word(heap_ptr, node);
				}				
				
				node = node->next;
			}
		}
	}


	pthread_exit((void *)1);
}

int find_number_of_line(unsigned long *lines, char *file) {

	FILE *fp;
    	char result[100];
	char command[255];
    	unsigned long i;
    	char c;	
	

	sprintf(command, "sudo wc -l %s >> lines.txt", file);
	
	system(command);

    	fp = fopen("lines.txt", "r");

    	if(!fp) {
        	fprintf(stderr, "Error opening file.\n");
        	return 1;
    	}

    	i = 0;
    	while((c = fgetc(fp)) != EOF && c!= ' ') {
        	result[i] = c;
        	i++;
    	}
    	result[i] = '\0';

    	i = atoi(result);

	*lines = i;

    	if (fclose(fp) == -1) {
        	fprintf(stderr," Error closing file!\n");
        	return 1;
    	}
	
	system("rm -rf lines.txt");

	return 0;

}

int main(int argc, char* argv[]) {

	int fd, fd1,fd2, err;
	void *t_ret;
	struct stat file_stat, file_stat1, file_stat2;
	char split_command[256];
	unsigned long no_of_lines = 0;

	if(argc == 1) {
		printf("Error: Enter the valid Input file\n");
		return -1;
	}
	
	if(argc > 3) {
		printf("Error: Input Should be :./wordcount file words_count");
		return -EINVAL;
	}

	if(argc == 3) {
		no_words = atoi(argv[2]);
		if(!no_words) {
			printf("Error : Enter the Valid Number\n");
			return -EINVAL;
		}
	}

	/* finding the number lines in a file to split it */
	if(find_number_of_line(&no_of_lines, argv[1])) {
		printf("Error: unable to know the file info for splitting\n");
		return -ENOENT;
	}

	sprintf(split_command, "split -l %d %s split_file", (no_of_lines/3) + 1, argv[1]);

	system(split_command);
	

	/* opening the split file 1 */
	fd = open("split_fileaa", O_RDONLY); 
	if(fd < 0) {
		printf("Error: Unable to open the file\n");
		return -ENOENT;
	}


	if((fstat(fd, &file_stat)) < 0) {
		printf("Error: Unable to read the file infomation\n");
		return -1;
	}	

	file_addr = (char *)mmap(0, file_stat.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
	if(file_addr == MAP_FAILED) {
		printf("Error: Unable to map the file\n");
		return -1;
	}	

	/* opening the split file 2 */
	fd1= open("split_fileab", O_RDONLY); 
	if(fd1< 0) {
		printf("Error: Unable to open the file\n");
		return -ENOENT;
	}


	if((fstat(fd1, &file_stat1)) < 0) {
		printf("Error: Unable to read the file infomation\n");
		return -1;
	}	

	file_addr1 = (char *)mmap(0, file_stat1.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd1, 0);
	if(file_addr1 == MAP_FAILED) {
		printf("Error: Unable to map the file\n");
		return -1;
	}

	/* opening the split file 3 */
	fd2= open("split_fileac", O_RDONLY); 
	if(fd2< 0) {
		printf("Error: Unable to open the file\n");
		return -ENOENT;
	}


	if((fstat(fd2, &file_stat2)) < 0) {
		printf("Error: Unable to read the file infomation\n");
		return -1;
	}	

	file_addr2 = (char *)mmap(0, file_stat2.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd2, 0);
	if(file_addr2 == MAP_FAILED) {
		printf("Error: Unable to map the file\n");
		return -1;
	}




	heap_node = (struct word_node*)malloc(no_words * sizeof(struct word_node));
	if(!heap_node) {
		printf("Error: Unable to allocate memory for Heap\n");
		return -ENOMEM; 
	}

	hash_table = (struct hash *) calloc(MAX_CHARS, sizeof(struct hash));
	if(!hash_table) {
		printf("Error: Unable to create hash table\n");
		return -ENOMEM;
	}


	/* assigning split file starting and ending address */
	file_start  = file_addr;
	file_end    = file_addr + file_stat.st_size;

	file_start1 = file_addr1;
	file_end1   = file_addr1 + file_stat1.st_size;

	file_start2 = file_addr2;
	file_end2   = file_addr2 + file_stat2.st_size;


	/* creating IO thread 1 to read split file 1 */
	err = pthread_create(&t1_io, NULL, parse_file1, NULL);
	if(err != 0) {
		printf("Error: Unable to create thread\n");
		return -1;
	}

	/* creating IO thread to read split file 2 */
	err = pthread_create(&t2_io, NULL, parse_file2, NULL);
	if(err != 0) {
		printf("Error: Unable to create thread\n");
		return -1;
	}

	/* creating IO thread to read split file 3 */
	err = pthread_create(&t3_io, NULL, parse_file3, NULL);
	if(err != 0) {
		printf("Error: Unable to create thread\n");
		return -1;
	}

	

	pthread_join(t1_io, &t_ret);
	pthread_join(t2_io, &t_ret);
	pthread_join(t3_io, &t_ret);

	err = pthread_create(&t1_cpu, NULL, find_top_occurences, (void *)heap_node);
	if(err != 0) {
		printf("Error: Unable to create thread\n");
		return -1;
	}

	pthread_join(t1_cpu, &t_ret);

	printf("\nTotal Unique Words : %ld\n", total_unique_words);
	printf("Total Valid Words : %ld\n", total_valid_words);


	if(argc == 3) {
		print_top_entries();	
	}

	free_hash_entries();

	munmap(file_addr, file_stat.st_size);
	free(hash_table);
	close(fd);
	free(heap_node);
	system("rm -f split_file*"); 	

	return 0;
}
