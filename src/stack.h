#ifndef S_STACK_H
#define S_STACK_H

struct s_stack;

/*
 * Create a new stack.
 *
 * Args:
 *          Max size of the stack.
 *
 * Returns:
 *          A stack pointer.
 *          NULL if malloc or calloc failed to allocate space to stack or 
 *              stack->data or max_stack_size is smaller than one.
 */
struct s_stack *stack_alloc(unsigned long max_stack_size);

/*
 * Push an element on the stack.
 *
 * Args:
 *          Stack pointer.
 *          A pointer to data that will be pushed on the stack.
 *          A function pointer that will free the allocated memory if the
 *              pointer points to allocated memory, for normal data types
 *              (char, int, uint8_t, etc) NULL should be used.
 *
 * Returns:
 *          0 if the element has been pushed succesfully on the stack.
 *          1 if calloc could not allocate more memory.
 *          2 if the stack is at its max size.
 *          3 if the stack pointer is NULL.
 */
int stack_push(struct s_stack *, void *, void (*) (void *));

/*
 * Pop an element off the stack.
 *
 * Args:
 *          Stack pointer.
 *
 * Returns:
 *          A void pointer to the data of popped element.
 *          NULL if the stack pointer is NULL or stack->size is zero
 *              or if realloc could not allocate memory.
 */
void *stack_pop(struct s_stack *);

/*
 * Add a new element on the stack that is a duplicate of the data of the stack.
 *
 * Args:
 *          Stack pointer.
 *
 * Returns:
 *          0 if the element has been pushed succesfully on the stack.
 *          1 if calloc could not allocate more memory.
 *          2 if the stack is at its max size.
 *          3 if the stack pointer is NULL.
 */
int stack_duplicate(struct s_stack *);

/*
 * Get the data of the element on top of the stack without removing it.
 *
 * Args:
 *          Stack pointer.
 *
 * Returns:
 *          Void pointer to the data on the stack.
 *          NULL if the stack pointer is NULL or the stack size is 0.
 */
void *stack_peek(struct s_stack *);

/*
 * Get the number of elements on the stack.
 */
unsigned long stack_size(struct s_stack *);

/*
 * Free all elements on the stack.
 *
 * This will not free the stack pointer that holds the elements.
 */
void stack_free(struct s_stack *);

#endif
