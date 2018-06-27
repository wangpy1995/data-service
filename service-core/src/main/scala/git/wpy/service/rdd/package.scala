package git.wpy.service

package object rdd {

  type ¬[A] = A => Nothing
  type ∨[T, U] = ¬[¬[T] with ¬[U]]
  type ¬¬[A] = ¬[¬[A]]
  type |∨|[T, U] = {type λ[X] = ¬¬[X] <:< (T ∨ U)}

  type NUM_TYPE = (Byte |∨| Short |∨| Int |∨| Long |∨| Float |∨| Double)

}
