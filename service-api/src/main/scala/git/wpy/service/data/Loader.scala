package git.wpy.service.data

trait Loader[V] {
  def doLoad(): V
}
