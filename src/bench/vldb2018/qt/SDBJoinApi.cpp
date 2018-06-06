#include "SDBJoinApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

#ifdef __sun 

void SDBJoinApi::hwJoin(dax_queue_t **queue, Node<Query>* node)
{ 
    int curPart = node->value.getPart();
    int col_id = this->baseColId + (curPart) * this->num_of_columns;
    column *lokey_col = &(this->leftTable->columns[col_id]);
    int num_of_els = lokey_col->c_meta.col_size;

    this->src.data = lokey_col->compressed;
    this->src.elements = num_of_els;
    this->dst.elements = num_of_els;

    void* join_vector = this->leftComp->getJoinBitVector1(curPart * this->num_of_segments);
    dst.data = join_vector; //get the memory block that will keep the join result

    node->t_start = gethrtime();
    node->post_data.t_start = gethrtime();
    node->post_data.node_ptr = (void *) node;

    dax_status_t join_status = dax_translate_post(*queue, flag, &src, &dst, &bit_map, src.elem_width, (void *) &(node->post_data));

    if(join_status != 0)
        printf("Dax Join Error! %d\n", join_status);
    else
        printf("Dax Join (%d), (%d)\n", curPart, node->value.getTableId());
}

#endif

