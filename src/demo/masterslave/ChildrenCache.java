/**
 * 
 */
package demo.masterslave;

import java.util.ArrayList;
import java.util.List;

/**
 * Àà/½Ó¿Ú×¢ÊÍ
 * 
 * @author linwn@ucweb.com
 * @createDate 2016-9-1
 * 
 */
public class ChildrenCache {

    protected List<String> children;
    
    public ChildrenCache(){
        this.children = null;
    }
    
    public ChildrenCache(List<String> children){
        this.children = children;
    }
    
    List<String> getList(){
        return this.children;
    }
    
    List<String> addedAndSet(List<String> newChildren){
        ArrayList<String> diff = null;
        if(this.children == null){
            diff = new ArrayList<String>(newChildren);
        }else{
            for(String s : newChildren){
                if(!this.children.contains(s)){
                    if(diff == null){
                        diff = new ArrayList<String>();
                    }
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
        return diff;
    }
    
    List<String> removeAndSet(List<String> newChildren){
        ArrayList<String> diff = null;
        if(children != null){
            for(String s : children){
                if(!newChildren.contains(s)){
                    if(diff == null){
                        diff = new ArrayList<String>();
                    }
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
        return diff;
    }
    
    
}
