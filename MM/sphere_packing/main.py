import numpy as np
from collections import deque

import matplotlib.pyplot as plt

class Atom:
    def __init__(self, position, wyckoff_position_name):
        self.position = position
        self.wyckoff_position_name = wyckoff_position_name

class Constraint:
    def __init__(self, coefficent_vector, bias):
        self.coefficent_vector = coefficent_vector
        self.bias = bias

    def check(self, atom_pos):
        value = self.coefficent_vector @ atom_pos
        return value > self.bias

    @staticmethod
    def parse(string):
        x, y, z, c = [float(i) for i in string.split(' ') if i != '']
        return Constraint(np.array([x, y, z]), c)
    

class Symmetry:
    def __init__(self, transformation_matrix, offset_vector):
        self.transformation_matrix = transformation_matrix
        self.offset_vector = offset_vector

    def apply(self, pos):
        return self.transformation_matrix @ pos + self.offset_vector

    @staticmethod
    def parse(string):
        items = [i for i in string.split(' ') if i != '']
        scheme = items[0]
        x, y, z, c = list(map(float, items[1:]))

        mat_temp = []
        for cc in scheme:
            cu = cc.upper()
            if cu == 'X':
                mat_temp.append([1, 0, 0] if cc == 'x' else [-1, 0, 0])
            elif cu == 'Y':
                mat_temp.append([0, 1, 0] if cc == 'y' else [0, -1, 0])
            elif cu == 'Z':
                mat_temp.append([0, 0, 1] if cc == 'z' else [0, 0, -1])

        return Symmetry(np.array(mat_temp).T, np.array([x, y, z]) / c)

class WyckoffPosition:
    def __init__(self, name, n_result_atoms, n_degrees_of_freedom, base_position, guide_vectors):
        self.name = name
        self.n_result_atoms = n_result_atoms
        self.n_degrees_of_freedom = n_degrees_of_freedom
        self.base_position = base_position
        self.guide_vectors = guide_vectors

    def get_position(self, parameters):
        pos = self.base_position.copy()
        for i in range(self.n_degrees_of_freedom):
            pos += self.guide_vectors[i] * parameters[i]
        
        return pos

    @staticmethod
    def parse(string):
        items = [i for i in string.split(' ') if i != '']
        pos_name = items[0]
        n_atoms = int(items[1])
        n_degree_of_freedom = int(items[2])

        x, y, z, c, *vecs_info = list(map(float, items[3:]))

        vecs = []
        for i in range(n_degree_of_freedom):
            vx, vy, vz = vecs_info[i*3:(i+1)*3]
            vecs.append(np.array([vx, vy, vz]) / c)

        return WyckoffPosition(pos_name, n_atoms, n_degree_of_freedom, np.array([x, y, z]) / c, vecs)

class AsymmetricUnit:
    def __init__(self, constraints, reverse_symmerties, symmetries, wyckoff_positions):
        self.reverse_symmerties = reverse_symmerties
        self.constraints = constraints
        self.symmetries = symmetries
        self.wyckoff_positions = wyckoff_positions
        self.eps = 1e-6

    @staticmethod
    def read_from_file(file):

        constraints = []
        reverse_symmetries = []
        symmetries = []
        wyckoff_positions = {}

        n = int(file.readline())
        for _ in range(n):
            constraints.append(Constraint.parse(file.readline().strip()))
        for _ in range(n):
            reverse_symmetries.append(Symmetry.parse(file.readline().strip()))

        n = int(file.readline())
        for _ in range(n):
            symmetries.append(Symmetry.parse(file.readline().strip()))

        n = int(file.readline())
        for _ in range(n):
            pos = WyckoffPosition.parse(file.readline().strip())
            wyckoff_positions[pos.name] = pos

        return AsymmetricUnit(constraints, reverse_symmetries, symmetries, wyckoff_positions)
    
    def check_constraints(self, pos):
        for constraint in self.constraints:
            if not constraint.check(pos):
                return False
        return True

    @staticmethod
    def distance(pos_a, pos_b):
        dx = abs(pos_a[0] - pos_b[0])
        dy = abs(pos_a[1] - pos_b[1])
        dz = abs(pos_a[2] - pos_b[2])
        
        if dx > 0.5:
            dx = 1 - dx

        if dy > 0.5:
            dy = 1 - dy

        if dz > 0.5:
            dz = 1 - dz

        return np.sqrt(dx * dx + dy * dy + dz * dz)

    @staticmethod
    def reduce_position(pos):
        return np.remainder(pos, 1)

    def check_distinct(self, pos, atoms):
        for atom in atoms:
            dist = AsymmetricUnit.distance(atom.position, pos)
            if dist < self.eps:
                return False
        return True

    def generate_atoms(self, parameters, check_count=True):
        atoms = [Atom(self.wyckoff_positions[wp_name].get_position(wp_params), wp_name) for wp_name, wp_params in parameters.items()]
        q = deque(atoms)
        while len(q) > 0:
            atom = q.popleft()
            for symmetry in self.symmetries:
                new_pos = symmetry.apply(atom.position)
                if not self.check_constraints(new_pos):
                    new_pos = AsymmetricUnit.reduce_position(new_pos)
                if self.check_distinct(new_pos, atoms):
                    new_atom = Atom(new_pos, atom.wyckoff_position_name)
                    atoms.append(new_atom)
                    q.append(new_atom)

        if check_count:
            right_atoms_count = 0
            for wp_name in parameters:
                right_atoms_count += self.wyckoff_positions[wp_name].n_result_atoms

            if len(atoms) < right_atoms_count:
                # print(f'warning: атомов меньше, чем нужно: есть {len(atoms)}, нужно {right_atoms_count}')
                return None

        return atoms

class DenseAtomPacker:
    def __init__(self, asymmetric_unit, radiuses_table):
        self.asymmetric_unit = asymmetric_unit
        self.radiuses_table = radiuses_table
    
    @staticmethod
    def absolute_distance(a, pos_a, pos_b):
        return a * AsymmetricUnit.distance(pos_a, pos_b)

    def overlap(self, a, atom_a, atom_b):
        radius_a = self.radiuses_table[atom_a.wyckoff_position_name]
        radius_b = self.radiuses_table[atom_b.wyckoff_position_name]

        dist = DenseAtomPacker.absolute_distance(a, atom_a.position, atom_b.position)

        return max((radius_a + radius_b) - dist, 0.0)

    def optimize_parameter_simple(self, a):
        parameter_low_bound = 0.0
        parameter_high_bound = 1.0

        atom_generation_parameters = {
            'b' : [],
            'c' : [], 
            'e' : [0.0]
        }

        n_samples = 1000
        parameter_step = (parameter_high_bound - parameter_low_bound) / n_samples

        params = []
        overlaps = []

        min_overlap = float('inf')
        optimal_atoms = None
        optimal_atom_generation_parameters = None
        for i in range(n_samples + 1):
            p = parameter_low_bound + i * parameter_step

            atom_generation_parameters['e'] = [p]

            atoms = self.asymmetric_unit.generate_atoms(atom_generation_parameters)

            if atoms is None:
                continue

            overlap = self.get_max_overlap(a, atoms)

            params.append(p)
            overlaps.append(overlap)

            if overlap < min_overlap:
                min_overlap = overlap
                optimal_atoms = atoms
                optimal_atom_generation_parameters = atom_generation_parameters.copy()

            if overlap == 0:
                break

        return min_overlap, optimal_atoms, optimal_atom_generation_parameters

    def optimize_parameter(self, a):
        offset = 0.001
        overlap_eps = 0.00001

        parameter_low_bound = 0.0
        parameter_high_bound = 1.0

        atom_generation_parameters = {
            'b' : [],
            'c' : [], 
            'e' : [0.0]
        }

        parameter = parameter_low_bound

        guide_vector_mag = np.linalg.norm(self.asymmetric_unit.wyckoff_positions['e'].guide_vectors[0])

        min_overlap = float('inf')
        optimal_atoms = None
        optimal_parameter = 0.0

        while True:
            atom_generation_parameters['e'][0] = parameter
            atoms = self.asymmetric_unit.generate_atoms(atom_generation_parameters)

            if atoms is None:
                parameter += offset
                continue

            if parameter > parameter_high_bound:
                break

            overlap = self.get_max_overlap(a, atoms)

            if overlap < min_overlap:
                min_overlap = overlap
                optimal_atoms = atoms
                optimal_parameter = parameter

            if overlap < overlap_eps:
                min_overlap = 0.0
                break
            else:
                parameter += overlap / (2 * a * guide_vector_mag)

            if parameter > parameter_high_bound:
                break
            
        atom_generation_parameters['e'][0] = optimal_parameter
        print(f'{min_overlap=}')
        return min_overlap, optimal_atoms, atom_generation_parameters


    def get_max_overlap(self, a, atoms):
        max_overlap = 0.0
        for atom_a in atoms:
            for atom_b in atoms:
                if atom_a is not atom_b:
                    max_overlap = max(max_overlap, self.overlap(a, atom_a, atom_b))
        return max_overlap


    def fit(self, a_min, a_max, eps):
        while a_max - a_min > eps:
            print(f'{a_min=} {a_max=}')
            a_mid = (a_max + a_min) / 2

            # overlap, atoms, atom_generation_parameters = self.optimize_parameter_simple(a_mid)
            overlap, atoms, atom_generation_parameters = self.optimize_parameter(a_mid)

            if overlap == 0:
                a_max = a_mid
            else:
                a_min = a_mid
            

        return (a_min + a_max) / 2, atom_generation_parameters, atoms


def load_asymmetric_units():
    asymmetric_units = {}
    with open('CUBIC.DAT') as file:
        while (line := file.readline().strip()) != 'HALT':
            asymmetric_units[line.strip()] = AsymmetricUnit.read_from_file(file)

    return asymmetric_units

asymmetric_units = load_asymmetric_units()

group_name = 'Fd3m'
asymmetric_unit = asymmetric_units[group_name]

xmin = 0
xmax = 1
ymin = 0
ymax = 1
zmin = 0
zmax = 1

atoms_generation_parameters = {
    'b' : [],
    'c' : [], 
    'e' : [0.1]
}

# crystal radiuses
radiuses_table = { 
    'b' : 0.75,  # http://abulafia.mt.ic.ac.uk/shannon/radius.php?Element=Fe
    'c' : 0.675, # http://abulafia.mt.ic.ac.uk/shannon/radius.php?Element=Al
    'e' : 1.26   # http://abulafia.mt.ic.ac.uk/shannon/radius.php?Element=O
}

packer = DenseAtomPacker(asymmetric_unit, radiuses_table)

eps = 0.001
a_min = 1
a_max = 15
lattice_constant, atom_generation_parameters, atoms = packer.fit(a_min, a_max, eps)

# должна быть, вроде a = 8.12 - 8.23 Å (http://rruff.geo.arizona.edu/AMS/minerals/Hercynite)
print(f'{lattice_constant=} {atom_generation_parameters=}')

palette = {
    'b' : 'r',
    'c' : 'g', 
    'e' : 'b'
}

from collections import defaultdict
atoms_grouped = defaultdict(list)
for a in atoms:
    atoms_grouped[a.wyckoff_position_name].append(a.position)

fig = plt.figure(figsize=(12,10))
ax = fig.add_subplot(projection='3d')
ax.set_xlim((xmin, xmax))
ax.set_ylim((ymin, ymax))
ax.set_zlim((zmin, zmax))
ax.scatter(*list(zip(*atoms_grouped['b'])), color=palette['b'])
ax.scatter(*list(zip(*atoms_grouped['c'])), color=palette['c'])
ax.scatter(*list(zip(*atoms_grouped['e'])), color=palette['e'])
ax.set_xlabel('x', fontsize=16)
ax.set_ylabel('y', fontsize=16)
ax.set_zlabel('z', fontsize=16)

plt.show()

wyckoff_pos_to_element_map = {
    'b' : 'Fe',
    'c' : 'Al', 
    'e' : 'O'
}

def save_result(file_name, wyckoff_pos_to_element_map, lattice_constant, atoms_grouped, radiuses_table):
    with open(file_name, 'w') as file:
        file.write(f'{lattice_constant}\n')
        file.write(f'{len(wyckoff_pos_to_element_map)}\n')
        for wp, e in wyckoff_pos_to_element_map.items():
            file.write(f'{e}\n')
            file.write(f'{radiuses_table[wp]}\n')

            atoms_in_group = atoms_grouped[wp]
            file.write(f'{len(atoms_in_group)}\n')
            for a in atoms_in_group:
                file.write(f'{a[0]} {a[1]} {a[2]}\n')

save_result('result.txt', wyckoff_pos_to_element_map, lattice_constant, atoms_grouped, radiuses_table)