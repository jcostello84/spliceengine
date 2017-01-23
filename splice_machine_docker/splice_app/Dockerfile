FROM base-mvn

# Adding the repo into the container.
ADD ./ spliceengine/

# There is still a lot to do. Ideally the .git directory would be removed to save space
# however, there are dependencies which rely on git history. TL;DR there is bunch of cleanup
# which should be done at this point, however there are a lot of dependencies in the 
# ./start-splice-cluster script which prevents cleanup.
