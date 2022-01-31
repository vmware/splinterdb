/* 
 * The implementation of the stack library.
 *
 * This stack implementation is void pointer based.
 */

 #include <stdlib.h>
 #include <assert.h>
 #include <string.h>
 #include "stack.h"

#define ELEM_SIZE sizeof(struct element)
#define SLAB_ALLOC_SIZE 256

/* 
 * The root of the stack.
 *
 * Holds the data pointer and contains the size of the stack, the
 * maximum size of the stack and how many memory has been allocated
 * to the stack.
 */
struct s_stack
{
    unsigned long size;
    unsigned long max_size;
    unsigned long size_allocated;
    struct element *data;
};

/*
 * Element needs a function pointer is the data pointed to by the data pointer
 * is allocated with malloc() or similar functions. For other 
 * standard data types NULL should suffice.
 */
struct element
{
    void *data;
    void (*free_data) (void *);
};

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
struct s_stack *stack_alloc(unsigned long max_stack_size)
{
    struct s_stack *stack = malloc(sizeof(struct s_stack));
    if ( stack == NULL || max_stack_size < 1)
        return NULL;

    unsigned long alloc_size = max_stack_size < SLAB_ALLOC_SIZE ?
        max_stack_size : SLAB_ALLOC_SIZE;

    stack->data = calloc(alloc_size, ELEM_SIZE);

    if ( stack->data == NULL )
        return NULL;

    stack->size = 0;
    stack->max_size = max_stack_size;
    stack->size_allocated = alloc_size;
    return stack;
}

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
int stack_push(struct s_stack *stack, void *data, void (*free_data) (void *))
{
    if ( stack == NULL )
        return 3;
    if ( stack->size == stack->max_size ) {
        return 2;
    } else if ( stack->size == stack->size_allocated ) {
        void *tmp = stack->data;
        stack->data = calloc(stack->size_allocated + SLAB_ALLOC_SIZE, ELEM_SIZE);
        if ( stack->data == NULL || tmp == NULL )
            return 1;
        memcpy(stack->data, tmp, stack->size_allocated);
        free(tmp);
        stack->size_allocated += SLAB_ALLOC_SIZE;
    }

    stack->size++;

    struct element new_elem = {data, free_data};
    struct element *old_elem = &(stack->data[stack->size - 1]);
    if ( old_elem->free_data != NULL )
        old_elem->free_data(data);
    *old_elem = new_elem;
    return 0;
}

// TODO(j0holo): Potential memory leak when alloc'd data is on the stack
// and the stack is resized to a smaller size.
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
void *stack_pop(struct s_stack *stack)
{
    if ( stack == NULL || stack->size == 0 )
        return NULL;

    stack->size--;
    // TODO(j0holo): 2 is a magic number.
    if ( stack->size < stack->size_allocated - 2 * SLAB_ALLOC_SIZE &&
        stack->size_allocated > 2 * SLAB_ALLOC_SIZE ) {
        
        void *tmp = stack->data;
        stack->data = calloc(stack->size_allocated - SLAB_ALLOC_SIZE, ELEM_SIZE);
        if ( stack->data == NULL || tmp == NULL )
            return NULL;
        memcpy(stack->data, tmp, stack->size_allocated - SLAB_ALLOC_SIZE);
        free(tmp);
        stack->size_allocated -= SLAB_ALLOC_SIZE;
    }
    struct element *elem = &(stack->data[stack->size]);
    return elem->data;
}

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
int stack_duplicate(struct s_stack *stack)
{
    struct element duplicate = stack->data[stack->size - 1];
    return stack_push(stack, duplicate.data, duplicate.free_data);
}

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
void *stack_peek(struct s_stack *stack) {
    if ( stack == NULL || stack->size == 0 )
        return NULL;
    struct element *elem = &(stack->data[stack->size - 1]);
    return elem->data;
}

/*
 * Get the number of elements on the stack.
 */
unsigned long stack_size(struct s_stack *stack)
{
    return stack->size;
}

/*
 * Free all elements on the stack.
 *
 * This will not free the stack pointer that holds the elements.
 * I'm thinking about changing this function so that the stack pointer
 * will be freed.
 */
void stack_free(struct s_stack *stack)
{
    assert(stack != NULL);

    for (unsigned long i = 0; i < stack->size_allocated; ++i)
    {
        if ( stack->data[i].free_data != NULL )
            stack->data[i].free_data(stack->data[i].data);
    }

    if ( stack->data != NULL )
        free(stack->data);
    stack->data = NULL;
    stack->size = 0;
    stack->size_allocated = 0;
}
