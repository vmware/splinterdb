
.cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053fd0 <clockcache_get>:
   53fd0:	push   %r15
   53fd2:	lea    -0x1(%rcx),%eax
   53fd5:	push   %r14
   53fd7:	push   %r13
   53fd9:	push   %r12
   53fdb:	push   %rbp
   53fdc:	push   %rbx
   53fdd:	mov    %rsi,%rbx
   53fe0:	sub    $0x38,%rsp
   53fe4:	mov    %ecx,0x4(%rsp)
   53fe8:	mov    %r8,0x10(%rsp)
   53fed:	cmp    $0x7,%eax
   53ff0:	ja     54458 <clockcache_get+0x488>
   53ff6:	mov    %rdi,%r15
   53ff9:	mov    %edx,%r12d
   53ffc:	mov    0x8(%r15),%rcx
   54000:	xor    %edx,%edx
   54002:	mov    0x8(%rcx),%rax
   54006:	mov    0x8(%rax),%r14
   5400a:	mov    %rbx,%rax
   5400d:	div    %r14
   54010:	test   %rdx,%rdx
   54013:	jne    544be <clockcache_get+0x4ee>
   54019:	mov    0x120(%rcx),%rcx
   54020:	mov    0x28(%r15),%rax
   54024:	mov    %rbx,%rdx
   54027:	shr    %cl,%rdx
   5402a:	shr    $0x2,%rax
   5402e:	cmp    %rax,%rdx
   54031:	jae    5448a <clockcache_get+0x4ba>
   54037:	mov    0x30(%r15),%rax
   5403b:	mov    (%rax,%rdx,4),%ebp
   5403e:	cmp    $0xffffffff,%ebp
   54041:	jne    54068 <clockcache_get+0x98>
   54043:	test   %r12d,%r12d
   54046:	jne    541c0 <clockcache_get+0x1f0>
   5404c:	mov    $0xb,%r13d
   54052:	add    $0x38,%rsp
   54056:	mov    %r13d,%eax
   54059:	pop    %rbx
   5405a:	pop    %rbp
   5405b:	pop    %r12
   5405d:	pop    %r13
   5405f:	pop    %r14
   54061:	pop    %r15
   54063:	ret
   54064:	nopl   0x0(%rax)
   54068:	data16 lea 0x80f38(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   54070:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   54078:	mov    %ebp,%esi
   5407a:	mov    %r15,%rdi
   5407d:	mov    (%rax),%rax
   54080:	mov    %rax,0x8(%rsp)
   54085:	test   %r12d,%r12d
   54088:	je     542d0 <clockcache_get+0x300>
   5408e:	call   aa320 <clockcache_try_get_read.constprop.0>
   54093:	cmp    $0x1,%eax
   54096:	jne    540ed <clockcache_get+0x11d>
   54098:	mov    $0x1,%r13d
   5409e:	movabs $0xcccccccccccccccd,%rax
   540a8:	mul    %r13
   540ab:	xor    %eax,%eax
   540ad:	shr    $0x2,%rdx
   540b1:	mov    %rdx,%r14
   540b4:	data16 cs nopw 0x0(%rax,%rax,1)
   540bf:	nop
   540c0:	pause
   540c2:	mov    %rax,%rcx
   540c5:	add    $0x1,%rax
   540c9:	cmp    %rcx,%r14
   540cc:	jne    540c0 <clockcache_get+0xf0>
   540ce:	cmp    $0x400,%r13
   540d5:	ja     54190 <clockcache_get+0x1c0>
   540db:	mov    %ebp,%esi
   540dd:	mov    %r15,%rdi
   540e0:	add    %r13,%r13
   540e3:	call   aa320 <clockcache_try_get_read.constprop.0>
   540e8:	cmp    $0x1,%eax
   540eb:	je     5409e <clockcache_get+0xce>
   540ed:	test   %eax,%eax
   540ef:	jne    53ffc <clockcache_get+0x2c>
   540f5:	mov    %ebp,%eax
   540f7:	mov    0x48(%r15),%rcx
   540fb:	mov    %rax,%r14
   540fe:	shl    $0x6,%r14
   54102:	mov    0x8(%rcx,%r14,1),%rdx
   54107:	cmp    %rdx,%rbx
   5410a:	jne    543cc <clockcache_get+0x3fc>
   54110:	mov    %rax,%rbx
   54113:	jmp    54124 <clockcache_get+0x154>
   54115:	nopl   (%rax)
   54118:	mov    %r15,%rdi
   5411b:	call   ea40 <clockcache_wait@plt>
   54120:	mov    0x48(%r15),%rcx
   54124:	mov    0x10(%rcx,%r14,1),%eax
   54129:	test   $0x10,%al
   5412b:	jne    54118 <clockcache_get+0x148>
   5412d:	mov    0x48(%r15),%rdx
   54131:	mov    %rbx,%rax
   54134:	add    %r14,%rdx
   54137:	mov    0x14(%rdx),%r13d
   5413b:	test   %r13d,%r13d
   5413e:	jne    54418 <clockcache_get+0x448>
   54144:	mov    0x20(%rdx),%eax
   54147:	cmp    %eax,0x4(%rsp)
   5414b:	jne    54421 <clockcache_get+0x451>
   54151:	mov    0x8(%r15),%rax
   54155:	mov    0x18(%rax),%esi
   54158:	test   %esi,%esi
   5415a:	jne    543a6 <clockcache_get+0x3d6>
   54160:	mov    %rdx,%r12
   54163:	mov    0x10(%rsp),%rbx
   54168:	xor    %r13d,%r13d
   5416b:	mov    %r12,(%rbx)
   5416e:	data16 lea 0x80e32(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   54176:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   5417e:	mov    (%rax),%rax
   54181:	mov    %rax,0x8(%rbx)
   54185:	jmp    54052 <clockcache_get+0x82>
   5418a:	nopw   0x0(%rax,%rax,1)
   54190:	mov    %ebp,%esi
   54192:	mov    %r15,%rdi
   54195:	call   aa320 <clockcache_try_get_read.constprop.0>
   5419a:	cmp    $0x1,%eax
   5419d:	jne    540ed <clockcache_get+0x11d>
   541a3:	xor    %eax,%eax
   541a5:	data16 cs nopw 0x0(%rax,%rax,1)
   541b0:	pause
   541b2:	mov    %rax,%rdx
   541b5:	add    $0x1,%rax
   541b9:	cmp    %rdx,%r14
   541bc:	jne    541b0 <clockcache_get+0x1e0>
   541be:	jmp    54190 <clockcache_get+0x1c0>
   541c0:	data16 lea 0x80de0(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   541c8:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   541d0:	mov    0x4(%rsp),%edx
   541d4:	mov    %rbx,%rsi
   541d7:	mov    %r15,%rdi
   541da:	mov    (%rax),%r13
   541dd:	call   53d00 <clockcache_acquire_entry_for_load.lto_priv.0>
   541e2:	mov    %rax,%rbp
   541e5:	mov    $0xffffffff,%eax
   541ea:	cmp    %rax,%rbp
   541ed:	je     53ffc <clockcache_get+0x2c>
   541f3:	mov    0x8(%r15),%rax
   541f7:	mov    %rbp,%r12
   541fa:	mov    %r13,0x8(%rsp)
   541ff:	shl    $0x6,%r12
   54203:	add    0x48(%r15),%r12
   54207:	mov    0x18(%rax),%ecx
   5420a:	test   %ecx,%ecx
   5420c:	jne    5437f <clockcache_get+0x3af>
   54212:	mov    0x18(%r15),%rdi
   54216:	mov    (%r12),%rsi
   5421a:	mov    %rbx,%rcx
   5421d:	mov    %r14,%rdx
   54220:	mov    (%rdi),%rax
   54223:	call   *(%rax)
   54225:	mov    %eax,%r13d
   54228:	test   %eax,%eax
   5422a:	jne    54349 <clockcache_get+0x379>
   54230:	mov    0x8(%r15),%rax
   54234:	mov    0x18(%rax),%edx
   54237:	test   %edx,%edx
   54239:	jne    542ec <clockcache_get+0x31c>
   5423f:	mov    %r13d,%ecx
   54242:	mov    %ebp,%edx
   54244:	mov    %rbx,%rsi
   54247:	mov    %r15,%rdi
   5424a:	call   52660 <clockcache_finish_load.lto_priv.0>
   5424f:	test   %r13d,%r13d
   54252:	je     54163 <clockcache_get+0x193>
   54258:	mov    0x8(%r15),%rcx
   5425c:	mov    %rbp,%rax
   5425f:	xor    %edx,%edx
   54261:	mov    0x8(%rsp),%rdi
   54266:	divq   0x140(%rcx)
   5426d:	and    $0x3,%edi
   54270:	shl    $0x6,%edx
   54273:	lea    (%rdx,%rax,1),%esi
   54276:	mov    0x130(%rcx),%eax
   5427c:	mov    0x88(%r15),%rdx
   54283:	mov    $0xffffffff,%ecx
   54288:	imul   %rdi,%rax
   5428c:	add    %rsi,%rax
   5428f:	lock xadd %cx,(%rdx,%rax,2)
   54295:	test   %cx,%cx
   54298:	jne    54052 <clockcache_get+0x82>
   5429e:	push   %rax
   5429f:	xor    %r9d,%r9d
   542a2:	xor    %eax,%eax
   542a4:	lea    0x63f45(%rip),%r8        # b81f0 <__func__.21+0xd30>
   542ab:	push   %rdi
   542ac:	lea    0x5d371(%rip),%rcx        # b1624 <_fini+0xa48>
   542b3:	lea    0x6e6d6(%rip),%rdx        # c2990 <__func__.28.lto_priv.1>
   542ba:	push   %rsi
   542bb:	lea    0x5d315(%rip),%rdi        # b15d7 <_fini+0x9fb>
   542c2:	mov    $0x261,%esi
   542c7:	push   0x20(%rsp)
   542cb:	call   ee00 <platform_assert_false@plt>
   542d0:	call   aa320 <clockcache_try_get_read.constprop.0>
   542d5:	cmp    $0x1,%eax
   542d8:	je     5404c <clockcache_get+0x7c>
   542de:	cmp    $0x2,%eax
   542e1:	je     53ffc <clockcache_get+0x2c>
   542e7:	jmp    540f5 <clockcache_get+0x125>
   542ec:	lea    0x20(%rsp),%rsi
   542f1:	mov    $0x1,%edi
   542f6:	call   e410 <clock_gettime@plt>
   542fb:	mov    0x18(%rsp),%rax
   54300:	imul   $0x3b9aca00,0x20(%rsp),%rdx
   54309:	add    0x28(%rsp),%rdx
   5430e:	sub    %rax,%rdx
   54311:	mov    0x8(%rsp),%rax
   54316:	lea    0x0(,%rax,8),%rcx
   5431e:	sub    %rax,%rcx
   54321:	mov    0x4(%rsp),%eax
   54325:	lea    (%rax,%rcx,8),%rax
   54329:	lea    (%r15,%rax,8),%rax
   5432d:	addq   $0x1,0x1148(%rax)
   54335:	addq   $0x1,0x1220(%rax)
   5433d:	add    %rdx,0x1190(%rax)
   54344:	jmp    5423f <clockcache_get+0x26f>
   54349:	mov    %eax,%edi
   5434b:	call   efe0 <strerror@plt>
   54350:	mov    0x80c79(%rip),%r14        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   54357:	mov    0x4(%rsp),%ecx
   5435b:	mov    %rbx,%rdx
   5435e:	mov    %rax,%r8
   54361:	lea    0x645c8(%rip),%rsi        # b8930 <__func__.21+0x1470>
   54368:	xor    %eax,%eax
   5436a:	mov    (%r14),%rdi
   5436d:	call   fbf0 <fprintf@plt>
   54372:	mov    (%r14),%rdi
   54375:	call   fe90 <fflush@plt>
   5437a:	jmp    54230 <clockcache_get+0x260>
   5437f:	lea    0x20(%rsp),%rsi
   54384:	mov    $0x1,%edi
   54389:	call   e410 <clock_gettime@plt>
   5438e:	imul   $0x3b9aca00,0x20(%rsp),%rax
   54397:	add    0x28(%rsp),%rax
   5439c:	mov    %rax,0x18(%rsp)
   543a1:	jmp    54212 <clockcache_get+0x242>
   543a6:	mov    0x8(%rsp),%rsi
   543ab:	mov    0x4(%rsp),%ecx
   543af:	lea    0x0(,%rsi,8),%rax
   543b7:	sub    %rsi,%rax
   543ba:	lea    0x220(%rcx,%rax,8),%rax
   543c2:	addq   $0x1,(%r15,%rax,8)
   543c7:	jmp    54160 <clockcache_get+0x190>
   543cc:	mov    0x8(%r15),%rcx
   543d0:	xor    %edx,%edx
   543d2:	mov    0x8(%rsp),%rdi
   543d7:	divq   0x140(%rcx)
   543de:	and    $0x3,%edi
   543e1:	shl    $0x6,%edx
   543e4:	lea    (%rdx,%rax,1),%esi
   543e7:	mov    0x130(%rcx),%eax
   543ed:	mov    0x88(%r15),%rdx
   543f4:	mov    $0xffffffff,%ecx
   543f9:	imul   %rdi,%rax
   543fd:	add    %rsi,%rax
   54400:	lock xadd %cx,(%rdx,%rax,2)
   54406:	test   %cx,%cx
   54409:	jne    53ffc <clockcache_get+0x2c>
   5440f:	jmp    5429e <clockcache_get+0x2ce>
   54414:	nopl   0x0(%rax)
   54418:	mov    0x8(%r15),%rcx
   5441c:	jmp    5425f <clockcache_get+0x28f>
   54421:	mov    0x4(%rsp),%esi
   54425:	mov    %ebp,%r9d
   54428:	lea    0x5d349(%rip),%r8        # b1778 <_fini+0xb9c>
   5442f:	lea    0x5d359(%rip),%rcx        # b178f <_fini+0xbb3>
   54436:	lea    0x6e373(%rip),%rdx        # c27b0 <__func__.19>
   5443d:	lea    0x5d193(%rip),%rdi        # b15d7 <_fini+0x9fb>
   54444:	push   %rsi
   54445:	mov    $0x8b6,%esi
   5444a:	push   %rax
   5444b:	xor    %eax,%eax
   5444d:	call   ee00 <platform_assert_false@plt>
   54452:	nopw   0x0(%rax,%rax,1)
   54458:	sub    $0x8,%rsp
   5445c:	lea    0x641ed(%rip),%r8        # b8650 <__func__.21+0x1190>
   54463:	xor    %eax,%eax
   54465:	push   %rsi
   54466:	mov    0x14(%rsp),%r9d
   5446b:	lea    0x6401e(%rip),%rcx        # b8490 <__func__.21+0xfd0>
   54472:	lea    0x6e307(%rip),%rdx        # c2780 <__func__.23.lto_priv.1>
   54479:	mov    $0x1af,%esi
   5447e:	lea    0x5d152(%rip),%rdi        # b15d7 <_fini+0x9fb>
   54485:	call   ee00 <platform_assert_false@plt>
   5448a:	imul   %r14,%rax
   5448e:	sub    $0x8,%rsp
   54492:	mov    %rbx,%r9
   54495:	mov    $0x195,%esi
   5449a:	lea    0x63de7(%rip),%r8        # b8288 <__func__.21+0xdc8>
   544a1:	lea    0x5d1a2(%rip),%rcx        # b164a <_fini+0xa6e>
   544a8:	lea    0x6e4c1(%rip),%rdx        # c2970 <__func__.33.lto_priv.0>
   544af:	lea    0x5d121(%rip),%rdi        # b15d7 <_fini+0x9fb>
   544b6:	push   %rax
   544b7:	xor    %eax,%eax
   544b9:	call   ee00 <platform_assert_false@plt>
   544be:	sub    $0x8,%rsp
   544c2:	mov    %rbx,%r9
   544c5:	lea    0x63d84(%rip),%r8        # b8250 <__func__.21+0xd90>
   544cc:	xor    %eax,%eax
   544ce:	push   %r14
   544d0:	lea    0x5d15d(%rip),%rcx        # b1634 <_fini+0xa58>
   544d7:	lea    0x6e492(%rip),%rdx        # c2970 <__func__.33.lto_priv.0>
   544de:	mov    $0x18e,%esi
   544e3:	lea    0x5d0ed(%rip),%rdi        # b15d7 <_fini+0x9fb>
   544ea:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
