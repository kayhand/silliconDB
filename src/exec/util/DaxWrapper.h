#ifndef __daxwrapper_h__
#define __daxwrapper_h__

#ifdef __sun
extern "C"{
    #include "/opt/dax/dax.h"
}
#endif

class DaxWrapper
{
    public:
        DaxWrapper(){};
        ~DaxWrapper(){};
   
        static dax_status_t createContext(dax_context_t **ctx);
   
};

dax_status_t DaxWrapper::createContext(dax_context_t **ctx){
    dax_status_t result = dax_thread_init(1, 1, 0, NULL, ctx);
    return result;
}
 
#endif
