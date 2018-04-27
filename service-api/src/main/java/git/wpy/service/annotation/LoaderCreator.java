package git.wpy.service.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface LoaderCreator {
    //类
    String loaderType();

}
