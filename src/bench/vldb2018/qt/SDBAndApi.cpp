#include "SDBAndApi.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <sys/time.h>

#ifdef __sun 

void SDBAndApi::hwAnd(dax_queue_t **queue, Node<Query>* node)
{ 
    int curPart = node->value.getPart();
    int col_id = this->baseColId + (curPart) * this->num_of_columns;
    column *lokey_col = &(this->leftTable->columns[col_id]);
    int num_of_els = lokey_col->c_meta.col_size;

    this->src.elements = num_of_els;
    this->src2.elements = num_of_els;
    this->dst.elements = num_of_els;

    //scan_result
    this->src.data = leftComp->getFilterBitVector(curPart * num_of_segments);
    this->src2.data = leftComp->getJoinBitVector1(curPart * num_of_segments);
    //this->dst.data = leftComp->getBitVector2(curPart * num_of_segments);

    node->t_start = gethrtime();
    node->post_data.t_start = gethrtime();
    node->post_data.node_ptr = (void *) node;

    dax_status_t and_status = dax_and_post(*queue, flag, &src, &src2, &dst, (void *) &(node->post_data));

    if(and_status != 0)
        printf("Dax And Error! %d\n", and_status);
    else
        printf("Dax And (%d), (%d)\n", curPart, node->value.getTableId());
}

#endif
