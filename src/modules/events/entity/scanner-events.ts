import { ApiProperty } from '@nestjs/swagger';
import { AbstractEntity } from 'src/common/abstract.entity';
import { Nodes } from 'src/modules/nodes/entity/nodes.entity';
import { Column, Entity } from 'typeorm';

@Entity('scanner_events')
export class ScannerEvents extends AbstractEntity {
  @ApiProperty()
  @Column('numeric', { name: 'signal_num' })
  signalNum: number;

  @ApiProperty()
  @Column('integer', { name: 'signal_id' })
  signalId: number;

  @ApiProperty()
  @Column('integer', { name: 'set_num' })
  setNum: number;

  @ApiProperty()
  @Column('numeric', { name: 'center_freq' })
  centerFreq: number;

  @ApiProperty()
  @Column('numeric', { name: 'bandwidth' })
  bandwidth: number;

  @ApiProperty()
  @Column('numeric', { name: 'elevation' })
  elevation: number;

  @ApiProperty()
  @Column('numeric', { name: 'azimuth' })
  azimuth: number;

  @ApiProperty()
  @Column('numeric', { name: 'signal_power' })
  signalPower: number;

  @ApiProperty()
  @Column('numeric', { name: 'signal_class' })
  signalClass: number;

  @ApiProperty()
  @Column({ name: 'status', type: 'integer' })
  status: number;

  @ApiProperty()
  @Column({ name: 'detail', type: 'text', nullable: true })
  detail: string;
}
