TARGET = utils
BUILD_DIR = build

PYENV_VERSION = $(shell cat ../.python-version)
PYENV_ROOT = $(HOME)/.pyenv
PYTHON_MAJOR_MINOR = $(shell echo $(PYENV_VERSION) | cut -d '.' -f 1,2)

INC = $(PYENV_ROOT)/versions/$(PYENV_VERSION)/include/python$(PYTHON_MAJOR_MINOR)
LIBS = $(PYENV_ROOT)/versions/$(PYENV_VERSION)/lib
LIB = python$(PYTHON_MAJOR_MINOR)

NUMPY_INCLUDE = $(PYENV_ROOT)/versions/$(PYENV_VERSION)/lib/python$(PYTHON_MAJOR_MINOR)/site-packages/numpy/core/include

all: clean init $(BUILD_DIR)/$(TARGET).so

$(BUILD_DIR)/$(TARGET).so: $(BUILD_DIR)/$(TARGET).o
	gcc -shared -o $(BUILD_DIR)/$(TARGET).so $(BUILD_DIR)/$(TARGET).o -L $(LIBS) -l $(LIB)

$(BUILD_DIR)/$(TARGET).o: $(BUILD_DIR)/$(TARGET).c
	gcc -O3 -o $(BUILD_DIR)/$(TARGET).o -c $(BUILD_DIR)/$(TARGET).c -I $(INC) -I $(NUMPY_INCLUDE) -fPIC

$(BUILD_DIR)/$(TARGET).c: $(TARGET).pyx
	cython -3 $(TARGET).pyx -o $(BUILD_DIR)/$(TARGET).c

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

.PHONY: init
init:
	@if [ ! -d "$(BUILD_DIR)" ]; then mkdir -p $(BUILD_DIR); fi