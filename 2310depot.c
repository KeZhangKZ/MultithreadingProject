#include <pthread.h>
#include <stdio.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <signal.h>

/**
 * Good struct stores good information. 
 * The information contains the name and quantity of this good. 
 */
typedef struct {
    char* name;
    int quantity;
} Good;

/**
 * Neighbour struct stores the information of neightbour depot,
 * The information contains the string of neighbour's name and
 * port and the file pointer that used to write to this neighbour)
 */
typedef struct {
    char* name;
    char* port;
    FILE* write;
} Neighbour;

/**
 * Instruction struct stores the information of one instruction.
 * The information contains the key of instruction (it is empty if the 
 * instruction is not a defer instruction), the good information (name,
 * quantity and destination, which is empty when it is not a transfer 
 * instruction)
 */
typedef struct {
    unsigned int key;
    char* type;
    int quantity;
    char* name;
    char* destination;
} Instruction;

/**
 * Depot struct stores the information of a depot information.
 * the information contains the lock, an array of thread ids with the size,
 * the name, port, array of goods (with size), array of neightbours (with size)
 * and array of instructions (with size).
 */
typedef struct {
    sem_t* lock;
    pthread_t* tids;
    int countTids;
    char* name;
    uint16_t port;
    Good** goods;
    int countGoods;
    Neighbour** neighbours;
    int countNeighbours;
    Instruction* instructions;
    int countInstructions;
} Depot;    

/**
 * Param struct stores the information of current connection and Depot.
 * the information of current connection contains the string of port, 
 * the file pointer of writing and reading for the connected depot
 * and the status that if has introduce self to the connected depot
 */
typedef struct {
    Depot* depot;
    char* port;
    FILE* write;
    FILE* read;
    bool hasIntro;
} Param;

/**
 * Parse the given msg if get the valid instruction,
 * do the corresponding instructions, else fail silently.
 * Parameters: param --- the pointer of struct Param, which provides the 
 *      depot information and current connection information
 *             msg --- the string of input msg that need to be parsed.
 */
void identify_msg(Param* param, char* msg);

/**
 * Exit program with the corresponding exit message according 
 * to the given exit number
 * Parameters: exitNumber --- a integer that indicate the chosen exit status 
 *      and exit number
 */
void exit_program(int exitNumber) {
    const char* messages[] = {"",
            "Usage: 2310depot name {goods qty}\n",
            "Invalid name(s)\n",
            "Invalid quantity\n"};
    fputs(messages[exitNumber], stderr);
    exit(exitNumber);
}

/**
 * Initialize the lock with initial number of thread that could take this lock.
 * the initial number of thread that could take the lock is 1.
 * Parameters: lock --- a sem_t pointer which indicates given semiphone lock
 *      that need to be initalized.
 */
void init_lock(sem_t* lock) {
    sem_init(lock, 0, 1);
}

/**
 * If the current the number of thread that could take this lock is greater
 * than 0, take lock with decrease 1. Otherwise, block and wait until the the
 * number of thread that could take this lock is greater than 0
 * Parameters: lock --- a sem_t pointer which indicates given semiphone lock
 *      that will be taken
 */
void take_lock(sem_t* lock) {
    sem_wait(lock);
}

/**
 * Release the given lock. and increase the number of thread that could take
 * the given lock by 1.
 * Parameters: lock --- a sem_t pointer which indicates given semiphone lock
 *      that will be released
 */
void release_lock(sem_t* lock) {
    sem_post(lock);
}

/**
 * Check if the number of program arguement is valid.
 * the number of program arguement should be even and greater than 2.
 * Otherwise, exit program with exit status 1.
 * Parameters: argc --- a integer that indicate the number of program arguement
 */
void check_argc(int argc) {
    //check if less than 2 args
    //check if argc is even
    if (argc < 2 || argc % 2 == 1) {
        exit_program(1);
    }
}

/**
 * Check if the given name is not empty and does not contain spaces, '\n',
 * '\r' and colons. if yes, return false. else, return true. The name might
 * be the name of good, neighbour, depot(self).
 * Parameters: name --- the string of given name that will be checked
 * Return: boolean value. true, if the given name is not empty and contains
 *      no banned characters. otherwise, false.
 */
bool check_name(char* name) {
    if (strlen(name) == 0) {
        return false;
    }
    for (int i = 0; i < strlen(name); i++) {
        if (name[i] == ' ' || name[i] == '\n' ||
                name[i] == '\r' || name[i] == ':') {
            return false;
        }
    }
    return true;
}

/**
 * Check if the given string of quantity is empty and try to convert it to
 * the qure integer. if it is valid, store the gathered integer to the given
 * int pointer and return true; otherwise, return false.
 * Parameters: quantityString --- the string of the given quantity that will be
 *      checked
 *             quantity --- the int pointer that will store the pure interger
 *      gathered
 * Return: boolean value. true, if the given string of quantity is a number and
 *      greater than 0. otherwise, false.
 */
bool check_quantity(char* quantityString, int* quantity) {
    if (strlen(quantityString) == 0) {
        return false;
    }
    char* err;
    *quantity = strtoul(quantityString, &err, 10);
    if (*err != '\0' || *quantity < 0) {
        return false;
    }
    return true;
}

/**
 * Check and store the program arguements. if the name of depot and goods
 * is valid and the quantity of goods is valid number, store those information
 * into the given depot struct. Otherwise, exit the program with 2 when names
 * are not valid and exit the program with 3 when the quantity is invalid.
 * Parameters: 
 *      argc: integer type, that indicate the number of program arguements
 *      argv: string array, which contains the program arguements
 *      depot: Depot struct pointer that used for storing the initial 
 *              information
 */
void process_args(int argc, char** argv, Depot* depot) {
    if (!check_name(argv[1])) {
        exit_program(2);
    }
    depot->name = argv[1];

    for (int i = 1; i < argc / 2; i++) {
        if (!check_name(argv[i * 2])) {
            exit_program(2);
        }
        Good* good = malloc(sizeof(Good));
        good->name = argv[i * 2];
        depot->goods[i - 1] = good;
    }

    for (int i = 1; i < argc / 2; i++) {
        if (!check_quantity((argv[i * 2 + 1]), 
                &(depot->goods[i - 1]->quantity))) {
            exit_program(3);
        }
    }
}

/**
 * A sorting helper function is to compare two goods according to their names.
 * Parameters:
 *      v1: constant void pointer which contains the Good struct pointer of
 *              first good
 *      v2: constant void pointer which contains the Good struct pointer of
 *              second good
 * Return: a integer, that indicate the order relation. return -1, if the first
 *      good is less than second good in lexicographic order. return 0, the 
 *      names of two goods are same. Return 1, if the first good is greater 
 *      than second good.
 */
int comp_goods(const void* v1, const void* v2) {
    Good* good1 = *((Good**) v1);
    Good* good2 = *((Good**) v2);
    return strcmp(good1->name, good2->name);
}

/**
 * A sorting helper function is to compare two neighbours according to 
 * their names.
 * Parameters:
 *      v1: constant void pointer which contains the Neighbour struct 
 *              pointer of first neighbour
 *      v2: constant void pointer which contains the Neighbour struct 
 *              pointer of second neighbour
 * Return: a integer, that indicate the order relation. return -1, if the first
 *      neighbour is less than second neighbour in lexicographic order. 
 *      return 0, the names of two neighbours are same. Return 1, if the first 
 *      neighbour is greater than second neighbour.
 */
int comp_neighbours(const void* v1, const void* v2) {
    Neighbour* neighbour1 = *((Neighbour**) v1);
    Neighbour* neighbour2 = *((Neighbour**) v2);
    return strcmp(neighbour1->name, neighbour2->name);
}

/**
 * Print the information of the given depot when the SIGHUP is recieved.
 * Parameters: void pointer, which contains the Depot struct pointer of 
 *      the depot whose information will be printed out.
 * Return: return 0 (pointer of null value);
 */
void* print_info(void* v) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGHUP);
    int num;
    while (!sigwait(&set, &num)) {
        Depot* depot = (Depot*) v;
        printf("Goods:\n");
        fflush(stdout);
        qsort(depot->goods, depot->countGoods, sizeof(Good*), comp_goods);
        for (int i = 0; i < depot->countGoods; i++) {
            if (depot->goods[i]->quantity) {
                printf("%s %d\n", depot->goods[i]->name, 
                        depot->goods[i]->quantity);
                fflush(stdout);
            }
        }

        printf("Neighbours:\n");
        fflush(stdout);
        qsort(depot->neighbours, depot->countNeighbours, sizeof(Neighbour*),
                comp_neighbours);
        for (int i = 0; i < depot->countNeighbours; i++) {
            if (strcmp(depot->neighbours[i]->name, "Invalid") != 0) {
                printf("%s\n", depot->neighbours[i]->name);
                fflush(stdout);
            }
        }
    }
    return 0;
}

/**
  * Read a line from the given file and return it
  * Parameters:
  *      file: the FILE* of given file that need to be read
  * Return: a string of a line of characters read from the given file pointer
  */
char* read_line(FILE* file) {
    char* line = (char*) malloc(sizeof(char) * 1);
    int length = 0;
    int next;

    while (1) {
        next = fgetc(file);
        if (next == EOF) {
            pthread_exit(0);
        } else if (next == '\n') {
            if (length == 0) {
                line[length++] = (char)next;
                line = (char*) realloc(line, sizeof(char) * (length + 1));
                line[length] = '\0';
                return line;
            } else {
                line[length] = '\0';
                return line;
            }
        } else {
            line[length++] = (char)next;
            line = (char*) realloc(line, sizeof(char) * (length + 1));
        }
    }
}

/**
 * Add a new empty neighbour (that will be added detail inforamtion) to 
 * the the depot of given connection param
 * Parameters: 
 *      param: a Param struct pointer which contains the depot pointer that
 *              used for adding a new empty neighbour
 */
void add_neighbour(Param* param) {
    Neighbour* newNeigh = malloc(sizeof(Neighbour));
    newNeigh->port = param->port;
    newNeigh->name = "Invalid";
    newNeigh->write = param->write;
    param->depot->neighbours[(param->depot->countNeighbours)++] = newNeigh;
    param->depot->neighbours = realloc(param->depot->neighbours, 
            sizeof(Neighbour*) * (param->depot->countNeighbours + 1));
}

/**
 * The thread function used to try to keep read a message from the connection
 * The connection information is involved in the given param
 * Parameter: 
 *      v: void pointer, which contains the Param struct pointer that contains
 *          the information of current connection and the depot information
 * Return: return 0 (pointer of null value);
 */
void* thread_function(void* v) {
    Param* param = (Param*) v;
    while (1) {
        char* msg = read_line(param->read);
        identify_msg(param, msg);
    }
    return 0;
}

/**
 * check if the given port has already connected with the given depot.
 * Parameters:
 *      depot: the Depot struct, which contains the information of all 
 *          neighbours information
 *      port: the string of port number that need to be checked
 * Return:
 *      bool value. true, if the given port has already connected with 
 *      the given depot. false, otherwise.
 */
bool check_connection(Depot* depot, char* port) {
    for (int i = 0; i < depot->countNeighbours; i++) {
        if (strcmp(depot->neighbours[i]->port, port) == 0) {
            return true;
        }
    }
    return false;
}

/**
 * Connect the depot according to the given port and keep recieving messages
 * from that depot.
 * Parameters:
 *      depot: the Depot struct, which contains the all the information of 
 *          given depot
 *      port: the string of port number that will be connected to  
 */
void connect_action(Depot* depot, char* port) {
    if (check_connection(depot, port)) {
        return;
    }
    struct addrinfo* aiCopy = 0;
    struct addrinfo hintsCopy;
    memset(&hintsCopy, 0, sizeof(struct addrinfo));
    hintsCopy.ai_family = AF_INET;
    hintsCopy.ai_socktype = SOCK_STREAM;
    int errCopy;
    if (errCopy = getaddrinfo("localhost", (const char*) port, &hintsCopy,
            &aiCopy), errCopy) {
        freeaddrinfo(aiCopy);
        fprintf(stderr, "prot issue---%s\n", gai_strerror(errCopy));
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0); // 0 == use default protocol
    if (connect(fd, (struct sockaddr*)aiCopy->ai_addr, 
            sizeof(struct sockaddr))) {
        perror("Connecting 1111");
        return;
    }
    int fd2 = dup(fd);
    FILE* to = fdopen(fd, "w");
    FILE* from = fdopen(fd2, "r");
    fprintf(to, "IM:%u:%s\n", depot->port, depot->name);
    fflush(to);
    
    Param* param = malloc(sizeof(Param));
    param->depot = depot;
    param->read = from;
    param->port = port;
    param->hasIntro = true;
    param->write = to;
    (depot->countTids)++;
    depot->tids = realloc(depot->tids, sizeof(pthread_t) * 
            (depot->countTids + 1));
    add_neighbour(param);    
    pthread_create(&(depot->tids[depot->countTids - 1]), 0, 
            thread_function, param);
}

/**
 * Introduce self to the connected depot according to the given connection 
 * param
 * Parameters:
 *      param: a Param struct pointer which contains the information of 
 *            connected depot (the write pointer and the name)
 *      name: a string of the name of the connected depot.
 */
void intro_action(Param* param, char* name) {
    if (!param->hasIntro) {
        fprintf(param->write, "IM:%u:%s\n", param->depot->port, 
                param->depot->name);
        fflush(param->write);
        param->hasIntro = true;
    }
    //store name
    for (int i = 0; i < param->depot->countNeighbours; i++) {
        if (param->depot->neighbours[i]->write == param->write) {
            param->depot->neighbours[i]->name = name;
            return;
        }
    }
}

/**
 * According to the given instruction, modify the good's quantity.
 * Parameters:
 *      depot: a Depot pointer, which provides all the goods information 
 *      instruction: a Instruction pointer, which stores the detail informaion
 *          of the instruction
 *      isAdd: a integer that indicates the good will be delivered or withdraws
 *              in given depot.
 */
void modify_self(Depot* depot, Instruction instruction, int isAdd) {
    for (int i = 0; i < depot->countGoods; i++) {
        if (strcmp(depot->goods[i]->name, instruction.name) == 0) {
            depot->goods[i]->quantity += (isAdd * instruction.quantity);
            return;
        }
    }
    //add new good with 0
    Good* newGood = malloc(sizeof(Good));
    newGood->name = instruction.name;
    newGood->quantity = isAdd * instruction.quantity;
    (depot->countGoods)++;
    depot->goods = realloc(depot->goods, sizeof(Good*) * depot->countGoods);
    depot->goods[depot->countGoods - 1] = newGood;
}

/**
 * Execute a specific given instruction (whose type should be Deliver, Withdraw
 * Transfer).
 * Parameters:
 *      depot: a Depot pointer, which provides all the goods information and 
 *          neighbour information.
 *      instruction: a Instruction pointer, which stores the detail informaion
 *          of the instruction will be executed
 */
void execute_instruction(Depot* depot, Instruction instruction) {
    if (strcmp(instruction.type, "Deliver") == 0) {
        modify_self(depot, instruction, 1);
    } else if (strcmp(instruction.type, "Withdraw") == 0) {
        modify_self(depot, instruction, -1);
    } else if (strcmp(instruction.type, "Transfer") == 0) {
        modify_self(depot, instruction, -1);
        for (int i = 0; i < depot->countNeighbours; i++) {
            if (strcmp(depot->neighbours[i]->name, 
                    instruction.destination) == 0) {
                fprintf(depot->neighbours[i]->write, "Deliver:%d:%s\n",
                        instruction.quantity, instruction.name);
                fflush(depot->neighbours[i]->write);
                return;
            }
        }
    }
}

/**
 * Execute all defer instructions with the given execute key.
 * Parameters:
 *      depot: a Depot pointer, which provides all the instructions contained
 *      key: a integer, which indicates the key of the defer instructions and 
 *              used to indentify the instructions
 */
void execute_action(Depot* depot, int key) {
    for (int i = 0; i < depot->countInstructions; i++) {
        if (depot->instructions[i].key == key && 
                strcmp(depot->instructions[i].type, "Invalid")) {
            execute_instruction(depot, depot->instructions[i]);
            depot->instructions[i].type = "Invalid";
        }
    }
}

/**
 * check if the message of instruction has redundant characters.
 * if yes, mark the given instruction as a invalid instruction.
 * Parameters: 
 *      instruction: a Instruction pointer, which stores the detail informaion
 *          of the instruction will be dealed with.
 *      delim: a string used to split the message, which is ":"
 */
void check_redundant(Instruction* instruction, char* delim) {
    if (strtok(NULL, delim) != NULL) {
        instruction->type = "Invalid";
    }
}

/**
 * Check the good information of recieved instruction. if there are valid name 
 * and quantity of good, store the information. Otherwise, mark the instruction
 * as invalid
 * Parameters:
 *      instruction: a Instruction pointer, which stores the detail informaion
 *          of the instruction will be dealed with.
 *      string: a string of the type of instruction
 *      delim: a string used to split the message, which is ":"
 */
void check_good_msg(Instruction* instruction, char* string, char* delim) {
    if (string = strtok(NULL, delim), string != NULL && check_quantity(string,
            &(instruction->quantity)) && instruction->quantity != 0) {
        if (string = strtok(NULL, delim), string != NULL && 
                check_name(string)) {
            instruction->name = string;
            return;
        }
    }
    instruction->type = "Invalid";
}

/**
 * Take basic actions for checking the basic instructions, Deliver, Withdraw 
 * and Transfer. if the recieved message is valid, store the instruction into
 * the given instruction. otherwise, mark the instruction as invalid.
 * Parameters:
 *      instruction: a Instruction pointer, which stores the detail informaion
 *          of the instruction will be dealed with.
 *      string: a string of the type of instruction
 *      delim: a string used to split the message, which is ":"
 */
void basic_action(Instruction* instruction, char* string, char* delim) {
    if (strcmp(string, "Deliver") == 0) {
        instruction->type = "Deliver";
        check_good_msg(instruction, string, delim);
        check_redundant(instruction, delim);
    } else if (strcmp(string, "Withdraw") == 0) {
        instruction->type = "Withdraw";
        check_good_msg(instruction, string, delim);
        check_redundant(instruction, delim);
    } else if (strcmp(string, "Transfer") == 0) {
        instruction->type = "Transfer";
        check_good_msg(instruction, string, delim);
        if (string = strtok(NULL, delim), string != NULL &&
                check_name(string)) {
            instruction->destination = string;
        }
        check_redundant(instruction, delim);
    } else {
        instruction->type = "Invalid";
    }
}

/**
 * Parse the given msg if get the valid instruction,
 * do the corresponding instructions, else fail silently.
 * Parameters: param --- the pointer of struct Param, which provides the 
 *      depot information and current connection information
 *             msg --- the string of input msg that need to be parsed.
 */
void identify_msg(Param* param, char* msg) {
    char delim[2] = ":";

    char* string = strtok(msg, delim);

    if (strcmp(string, "Connect") == 0) {
        if (string = strtok(NULL, delim), string != NULL &&
                strtok(NULL, delim) == NULL) {
            connect_action(param->depot, string);
        }
    } else {
        take_lock(param->depot->lock);
        if (strcmp(string, "IM") == 0) {
            char* port = strtok(NULL, delim);
            string = strtok(NULL, delim);
            if (port != NULL && string != NULL &&
                    check_name(string) && strtok(NULL, delim) == NULL) {
                intro_action(param, string);
            }
        } else if (strcmp(string, "Execute") == 0) {
            unsigned int key;
            if (string = strtok(NULL, delim), string != NULL &&
                    check_quantity(string, (int*)&key)) {
                execute_action(param->depot, key);
            }
        } else if (strcmp(string, "Defer") == 0) {
            Instruction instruction;
            if (string = strtok(NULL, delim), string != NULL &&
                    check_quantity(string, (int*) &(instruction.key))) {
                string = strtok(NULL, delim);
                basic_action(&instruction, string, delim);
                param->depot->countInstructions++;
                param->depot->instructions = 
                        realloc(param->depot->instructions, 
                        sizeof(Instruction) * 
                        (param->depot->countInstructions + 1));
                param->depot->instructions[(param->depot->countInstructions)
                        - 1] = instruction;
            }
        } else {
            Instruction instruction;
            basic_action(&instruction, string, delim);    
            execute_instruction(param->depot, instruction);
        }
        release_lock(param->depot->lock);    
    }
}

/**
 * bind a random port and accept the connections and recieve messages
 * Parameter:
 *      v: void pointer which contains a Depot struct pointer that 
 *          contains the depot information
 * Return: return 0 (pointer of null value)
 */
void* bind_port(void* v) {
    Depot* depot = (Depot*) v;
    struct addrinfo* ai = 0;
    struct addrinfo hints;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;        // IPv6  for generic could use AF_UNSPEC
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;  // Because we want to bind with it
    int err;
    if (err = getaddrinfo("localhost", 0, &hints, &ai), err) {
        freeaddrinfo(ai);
    }

    int serv = socket(AF_INET, SOCK_STREAM, 0); // 0 == use default protocol
    bind(serv, (struct sockaddr*)ai->ai_addr, sizeof(struct sockaddr));

    struct sockaddr_in ad;
    memset(&ad, 0, sizeof(struct sockaddr_in));
    socklen_t len = sizeof(struct sockaddr_in);
    getsockname(serv, (struct sockaddr*)&ad, &len);
    depot->port = ntohs(ad.sin_port);
    printf("%u\n", depot->port);
    fflush(stdout);

    listen(serv, 10);
    
    int connFd;
    while (connFd = accept(serv, 0, 0), connFd >= 0) {
        int fd = dup(connFd);
        Param* param = malloc(sizeof(Param));
        param->depot = depot;
        param->read = fdopen(connFd, "r");
        param->write = fdopen(fd, "w");
        param->hasIntro = false;
        take_lock(param->depot->lock);
        (depot->countTids)++;
        depot->tids = realloc(depot->tids, sizeof(pthread_t) *
                (depot->countTids + 1));
        add_neighbour(param);    
        release_lock(param->depot->lock);
        pthread_create(&(depot->tids[depot->countTids - 1]), 0,
                thread_function, param);
    }
    return 0;
}

int main(int argc, char** argv) {
    check_argc(argc);

    Depot* depot = malloc(sizeof(Depot));
    depot->lock = malloc(sizeof(sem_t));
    init_lock(depot->lock);
    depot->countGoods = argc / 2 - 1;
    depot->countNeighbours = 0;
    depot->countInstructions = 0;
    depot->countTids = 0;
    depot->tids = malloc(sizeof(pthread_t));
    depot->goods = malloc(sizeof(Good*) * depot->countGoods);
    depot->neighbours = malloc(sizeof(Neighbour*));
    depot->instructions = malloc(sizeof(Instruction));
    process_args(argc, argv, depot);

    pthread_t tid;
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGHUP);
    pthread_sigmask(SIG_BLOCK, &set, 0);

    pthread_create(&tid, 0, print_info, depot);
    pthread_create(&tid, 0, bind_port, depot);
    pthread_exit(0);
}
