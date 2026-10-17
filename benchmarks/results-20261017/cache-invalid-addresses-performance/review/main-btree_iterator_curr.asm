
.cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000405a0 <btree_iterator_curr>:
   405a0:	push   %r12
   405a2:	mov    %rsi,%r12
   405a5:	push   %rbp
   405a6:	mov    %rdx,%rbp
   405a9:	push   %rbx
   405aa:	mov    %rdi,%rbx
   405ad:	sub    $0x30,%rsp
   405b1:	cmpq   $0x0,0x70(%rdi)
   405b6:	je     405cd <btree_iterator_curr+0x2d>
   405b8:	mov    0x8(%rdi),%rdi
   405bc:	mov    0x68(%rbx),%rdx
   405c0:	lea    0x70(%rbx),%rsi
   405c4:	mov    (%rdi),%rax
   405c7:	call   *0xd0(%rax)
   405cd:	mov    0x80(%rbx),%rax
   405d4:	mov    0x1c(%rbx),%edx
   405d7:	cmpb   $0x0,0x20(%rax)
   405db:	jne    40640 <btree_iterator_curr+0xa0>
   405dd:	cmp    $0x2,%edx
   405e0:	je     407e0 <btree_iterator_curr+0x240>
   405e6:	movzwl 0x90(%rbx),%edx
   405ed:	movzwl 0x28(%rax,%rdx,2),%ecx
   405f2:	add    %rax,%rcx
   405f5:	movzwl 0x26(%rax),%eax
   405f9:	movzwl (%rcx),%esi
   405fc:	test   %ax,%ax
   405ff:	jne    40740 <btree_iterator_curr+0x1a0>
   40605:	lea    0x6(%rcx),%rdx
   40609:	mov    %rsi,%rax
   4060c:	mov    %rdx,%rdi
   4060f:	movq   $0x2,(%r12)
   40617:	add    %rsi,%rdx
   4061a:	movzwl 0x3(%rcx),%esi
   4061e:	mov    %rax,0x8(%r12)
   40623:	mov    %rdi,0x10(%r12)
   40628:	movzbl 0x5(%rcx),%eax
   4062c:	xor    %ecx,%ecx
   4062e:	test   $0x4,%al
   40630:	je     40636 <btree_iterator_curr+0x96>
   40632:	mov    0x8(%rbx),%rcx
   40636:	and    $0x3,%eax
   40639:	jmp    40722 <btree_iterator_curr+0x182>
   4063e:	xchg   %ax,%ax
   40640:	cmp    $0x2,%edx
   40643:	je     407b0 <btree_iterator_curr+0x210>
   40649:	movzwl 0x90(%rbx),%edx
   40650:	movzwl 0x28(%rax,%rdx,2),%edx
   40655:	add    %rdx,%rax
   40658:	movzwl 0x14(%rax),%edx
   4065c:	cmp    $0xffff,%dx
   40660:	je     407c5 <btree_iterator_curr+0x225>
   40666:	cmp    $0xfffe,%dx
   4066a:	je     407f0 <btree_iterator_curr+0x250>
   40670:	lea    0x17(%rax),%rcx
   40674:	movzwl %dx,%esi
   40677:	mov    $0x2,%edi
   4067c:	mov    %rcx,0x10(%r12)
   40681:	pxor   %xmm3,%xmm3
   40685:	movq   (%rax),%xmm2
   40689:	mov    %edi,(%r12)
   4068d:	mov    0x10(%rax),%ecx
   40690:	movl   $0x0,0x4(%r12)
   40699:	mov    %rsi,0x8(%r12)
   4069e:	movq   0x8(%rax),%xmm0
   406a3:	punpckldq %xmm3,%xmm0
   406a7:	testb  $0x1,0x16(%rax)
   406ab:	je     40760 <btree_iterator_curr+0x1c0>
   406b1:	add    $0x17,%rax
   406b5:	movzwl %dx,%esi
   406b8:	add    %rax,%rsi
   406bb:	cmp    $0xfffd,%dx
   406bf:	cmovbe %rsi,%rax
   406c3:	movq   (%rax),%xmm1
   406c7:	mov    0x8(%rax),%eax
   406ca:	shl    $0x20,%rax
   406ce:	movq   %xmm1,(%rsp)
   406d3:	punpckldq %xmm3,%xmm1
   406d7:	or     %rcx,%rax
   406da:	psllq  $0x20,%xmm1
   406df:	mov    %rax,0x20(%rsp)
   406e4:	por    %xmm0,%xmm1
   406e8:	movaps %xmm1,0x10(%rsp)
   406ed:	movdqa %xmm2,%xmm1
   406f1:	mov    $0x3e8,%eax
   406f6:	xor    %ecx,%ecx
   406f8:	mov    $0x20,%esi
   406fd:	movq   0x18(%rsp),%xmm0
   40703:	movhps 0x10(%rsp),%xmm1
   40708:	lea    0x138(%rbx),%rdx
   4070f:	movups %xmm1,0x138(%rbx)
   40716:	movhps 0x20(%rsp),%xmm0
   4071b:	movups %xmm0,0x148(%rbx)
   40722:	mov    %eax,0x0(%rbp)
   40725:	mov    %rcx,0x8(%rbp)
   40729:	mov    %rsi,0x10(%rbp)
   4072d:	mov    %rdx,0x18(%rbp)
   40731:	add    $0x30,%rsp
   40735:	pop    %rbx
   40736:	pop    %rbp
   40737:	pop    %r12
   40739:	ret
   4073a:	nopw   0x0(%rax,%rax,1)
   40740:	mov    0xc0(%rbx),%rdx
   40747:	add    %rsi,%rax
   4074a:	mov    0x50(%rdx),%rdi
   4074e:	lea    0x6(%rcx),%rdx
   40752:	jmp    4060f <btree_iterator_curr+0x6f>
   40757:	nopw   0x0(%rax,%rax,1)
   40760:	mov    $0x7fffffff,%eax
   40765:	cmp    $0x7fffffff,%rcx
   4076c:	movq   %rax,%xmm1
   40771:	mov    $0xffffffffffffffff,%rax
   40778:	punpcklqdq %xmm1,%xmm1
   4077c:	cmove  %rax,%rcx
   40780:	pcmpeqd %xmm0,%xmm1
   40784:	mov    %rcx,0x20(%rsp)
   40789:	pshufd $0xb1,%xmm1,%xmm3
   4078e:	pand   %xmm3,%xmm1
   40792:	movdqa %xmm1,%xmm3
   40796:	pandn  %xmm0,%xmm3
   4079a:	por    %xmm1,%xmm3
   4079e:	movaps %xmm3,0x10(%rsp)
   407a3:	jmp    406ed <btree_iterator_curr+0x14d>
   407a8:	nopl   0x0(%rax,%rax,1)
   407b0:	mov    0xb8(%rbx),%rax
   407b7:	movzwl 0x14(%rax),%edx
   407bb:	cmp    $0xffff,%dx
   407bf:	jne    40666 <btree_iterator_curr+0xc6>
   407c5:	xor    %ecx,%ecx
   407c7:	mov    $0xffffffffffffffff,%rsi
   407ce:	mov    $0x1,%edi
   407d3:	jmp    4067c <btree_iterator_curr+0xdc>
   407d8:	nopl   0x0(%rax,%rax,1)
   407e0:	mov    0xb8(%rbx),%rcx
   407e7:	jmp    405f5 <btree_iterator_curr+0x55>
   407ec:	nopl   0x0(%rax)
   407f0:	xor    %ecx,%ecx
   407f2:	mov    $0xffffffffffffffff,%rsi
   407f9:	mov    $0x3,%edi
   407fe:	jmp    4067c <btree_iterator_curr+0xdc>

Disassembly of section .fini:
