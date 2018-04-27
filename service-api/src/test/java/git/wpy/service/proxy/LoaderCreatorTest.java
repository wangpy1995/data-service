package git.wpy.service.proxy;

import git.wpy.service.annotation.LoaderCreator;

@LoaderCreator(loaderType = "nothing")
public class LoaderCreatorTest {
    public void createLoader() {
        System.out.println("create loader.");
    }
}