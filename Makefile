CC = g++
DDBUG = -g 
INC = ./src
INCDIRS = -I${INC}
SRC_DIR = ./src
CFLAGS = ${INCDIRS} ${DDBUG}

all: main

clean:
	rm -f $(SRC_DIR)/*.o main

SRC = $(SRC_DIR)/main.cc $(SRC_DIR)/sch_int.cc $(SRC_DIR)/recovery_ctrl.cc $(SRC_DIR)/dic_ctrl.cc\
      $(SRC_DIR)/l2_delta_ctrl.cc	$(SRC_DIR)/l1_delta_ctrl.cc $(SRC_DIR)/io_ctrl.cc
      
OBJ = $(SRC:.cc=.o)  

$(OBJ): $(SRC_DIR)/%.o: $(SRC_DIR)/%.cc
	$(CC) -c $(CFLAGS) $< -o $@

main : $(SRC_DIR)/main.o $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ)