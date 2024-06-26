import {Column, Entity, ManyToOne, PrimaryGeneratedColumn} from 'typeorm';

@Entity('post_category')
export class PostCategory {

  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  name: string;
}